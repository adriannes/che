/*******************************************************************************
 * Copyright (c) 2012-2016 Codenvy, S.A.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *   Codenvy, S.A. - initial API and implementation
 *******************************************************************************/
package org.eclipse.che.api.workspace.server;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.eclipse.che.api.core.ConflictException;
import org.eclipse.che.api.core.NotFoundException;
import org.eclipse.che.api.core.ServerException;
import org.eclipse.che.api.core.model.machine.Machine;
import org.eclipse.che.api.core.model.machine.MachineLogMessage;
import org.eclipse.che.api.core.model.workspace.Environment;
import org.eclipse.che.api.core.model.workspace.WorkspaceRuntime;
import org.eclipse.che.api.core.model.workspace.WorkspaceStatus;
import org.eclipse.che.api.core.notification.EventService;
import org.eclipse.che.api.core.notification.EventSubscriber;
import org.eclipse.che.api.core.util.AbstractLineConsumer;
import org.eclipse.che.api.core.util.AbstractMessageConsumer;
import org.eclipse.che.api.core.util.LineConsumer;
import org.eclipse.che.api.core.util.MessageConsumer;
import org.eclipse.che.api.core.util.WebsocketMessageConsumer;
import org.eclipse.che.api.environment.server.EnvironmentStartException;
import org.eclipse.che.api.environment.server.spi.EnvironmentEngine;
import org.eclipse.che.api.machine.server.model.impl.MachineImpl;
import org.eclipse.che.api.machine.server.model.impl.MachineLogMessageImpl;
import org.eclipse.che.api.machine.shared.dto.event.MachineStatusEvent;
import org.eclipse.che.api.workspace.server.model.impl.EnvironmentImpl;
import org.eclipse.che.api.workspace.server.model.impl.WorkspaceImpl;
import org.eclipse.che.api.workspace.server.model.impl.WorkspaceRuntimeImpl;
import org.eclipse.che.api.workspace.shared.dto.event.WorkspaceStatusEvent;
import org.eclipse.che.api.workspace.shared.dto.event.WorkspaceStatusEvent.EventType;
import org.slf4j.Logger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.eclipse.che.api.machine.shared.Constants.ENVIRONMENT_OUTPUT_CHANNEL_TEMPLATE;
import static org.eclipse.che.dto.server.DtoFactory.newDto;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Defines an internal API for managing {@link WorkspaceRuntimeImpl} instances.
 *
 * <p>This component implements {@link WorkspaceStatus} contract.
 *
 * <p>All the operations performed by this component are synchronous.
 *
 * <p>The implementation is thread-safe and guarded by
 * eagerly initialized readwrite locks produced by {@link StrippedLocks}.
 * The component doesn't expose any api for client-side locking.
 * All the instances produced by this component are copies of the real data.
 *
 * <p>The component doesn't check if the incoming objects are in application-valid state.
 * Which means that it is expected that if {@link #start(WorkspaceImpl, String)} method is called
 * then {@code WorkspaceImpl} argument is a application-valid object which contains
 * all the required data for performing start.
 *
 * @author Yevhenii Voevodin
 * @author Alexander Garagatyi
 */
@Singleton
public class WorkspaceRuntimes {

    private static final Logger LOG = getLogger(WorkspaceRuntimes.class);

    @VisibleForTesting
    final         Map<String, RuntimeDescriptor>      descriptors;
    @VisibleForTesting
    private final EventService                        eventService;
    private final EventSubscriber<MachineStatusEvent> removeMachineEventSubscriber;
    private final Map<String, EnvironmentEngine>      environmentEngines;
    private final StrippedLocks                       strippedLocks;

    private volatile boolean isPreDestroyInvoked;

    @Inject
    public WorkspaceRuntimes(EventService eventService, Map<String, EnvironmentEngine> environmentEngines) {
        this.eventService = eventService;
        this.environmentEngines = environmentEngines;
        this.descriptors = new HashMap<>();
        this.removeMachineEventSubscriber = new RemoveMachineEventSubscriber();
        // 16 - experimental value for stripes count, it comes from default hash map size
        this.strippedLocks = new StrippedLocks(16);
    }

    /**
     * Returns the runtime descriptor describing currently starting/running/stopping
     * workspace runtime.
     *
     * <p>Note that the {@link RuntimeDescriptor#getRuntime()} method
     * returns a copy of a real {@code WorkspaceRuntime} object,
     * which means that any runtime copy modifications won't affect the
     * real object and also it means that copy won't be affected with modifications applied
     * to the real runtime workspace object state.
     *
     * @param workspaceId
     *         the id of the workspace to get its runtime
     * @return descriptor which describes current state of the workspace runtime
     * @throws NotFoundException
     *         when workspace with given {@code workspaceId} is not running
     */
    public RuntimeDescriptor get(String workspaceId) throws NotFoundException {
        try (StrippedLocks.ReadLock lock = strippedLocks.acquireReadLock(workspaceId)) {
            final RuntimeDescriptor descriptor = descriptors.get(workspaceId);
            if (descriptor == null) {
                throw new NotFoundException("Workspace with id '" + workspaceId + "' is not running.");
            }
            return new RuntimeDescriptor(descriptor);
        }
    }

    /**
     * Starts all machines from specified workspace environment,
     * creates workspace runtime instance based on that environment.
     *
     * <p>During the start of the workspace its
     * runtime is visible with {@link WorkspaceStatus#STARTING} status.
     *
     * @param workspace
     *         workspace which environment should be started
     * @param envName
     *         the name of the environment to start
     * @param recover
     *         whether machines should be recovered(true) or not(false)
     * @return the workspace runtime instance with machines set.
     * @throws ConflictException
     *         when workspace is already running
     * @throws ConflictException
     *         when start is interrupted
     * @throws NotFoundException
     *         when any not found exception occurs during environment start
     * @throws ServerException
     *         when component {@link #isPreDestroyInvoked is stopped} or any
     *         other error occurs during environment start
     * @see EnvironmentEngine#start(String, Environment, boolean, MessageConsumer)
     * @see WorkspaceStatus#STARTING
     * @see WorkspaceStatus#RUNNING
     */
    public RuntimeDescriptor start(WorkspaceImpl workspace,
                                   String envName,
                                   boolean recover) throws ServerException,
                                                           ConflictException,
                                                           NotFoundException {
        String workspaceId = workspace.getId();

        Optional<EnvironmentImpl> environmentOpt = workspace.getConfig().getEnvironment(envName);
        if (!environmentOpt.isPresent()) {
            throw new IllegalArgumentException(format("Workspace '%s' doesn't contain environment '%s'",
                                                      workspaceId,
                                                      envName));
        }

        // Environment copy constructor makes deep copy of objects graph
        // in this way machine configs also copied from incoming values
        // which means that original values won't affect the values in starting queue
        final EnvironmentImpl environmentCopy = new EnvironmentImpl(environmentOpt.get());

        EnvironmentEngine engine = environmentEngines.get(environmentCopy.getRecipe().getType());
        if (engine == null) {
            throw new NotFoundException("Environment engine of type '" + environmentCopy.getRecipe().getType() + "' is not found");
        }

        // This check allows to exit with an appropriate exception before blocking on lock.
        // The double check is required as it is still possible to get unlucky timing
        // between locking and starting workspace.
        ensurePreDestroyIsNotExecuted();
        try (StrippedLocks.WriteLock lock = strippedLocks.acquireWriteLock(workspaceId)) {
            ensurePreDestroyIsNotExecuted();
            final RuntimeDescriptor existingDescriptor = descriptors.get(workspaceId);
            if (existingDescriptor != null) {
                throw new ConflictException(format("Could not start workspace '%s' because its status is '%s'",
                                                   workspace.getConfig().getName(),
                                                   existingDescriptor.getRuntimeStatus()));
            }

            // Create a new runtime descriptor and save it with 'STARTING' status
            final RuntimeDescriptor descriptor = new RuntimeDescriptor(new WorkspaceRuntimeImpl(envName,
                                                                                                environmentCopy.getRecipe().getType()));
            descriptor.setRuntimeStatus(WorkspaceStatus.STARTING);
            descriptors.put(workspaceId, descriptor);
        }

        ensurePreDestroyIsNotExecuted();
        publishEvent(EventType.STARTING, workspaceId, null);

        String environmentStartError;
        try {
            List<Machine> machines = engine.start(workspaceId,
                                                  environmentCopy,
                                                  recover,
                                                  getEnvironmentLogger(workspaceId));

            List<MachineImpl> machinesImpls = machines.stream()
                                                      .map(MachineImpl::new)
                                                      .collect(Collectors.toList());
            Optional<MachineImpl> devMachineOpt = machinesImpls.stream()
                                                               .filter(machine -> machine.getConfig().isDev())
                                                               .findAny();

            if (devMachineOpt.isPresent()) {
                // todo check that it is not stopped
                try (StrippedLocks.WriteLock lock = strippedLocks.acquireWriteLock(workspaceId)) {
                    RuntimeDescriptor descriptor = descriptors.get(workspaceId);
                    WorkspaceRuntimeImpl runtime = descriptor.getRuntime();

                    descriptor.setRuntimeStatus(WorkspaceStatus.RUNNING);
                    runtime.setMachines(machinesImpls);
                    runtime.setDevMachine(devMachineOpt.get());
                }
                // Event publication should be performed outside of the lock
                // as it may take some time to notify subscribers
                publishEvent(EventType.RUNNING, workspaceId, null);
                return get(workspaceId);
            } else {
                environmentStartError = "Environment " +
                                        envName +
                                        " has booted but it doesn't contain dev machine. Environment has been stopped.";
                try {
                    engine.stop(workspaceId);
                } catch (Exception e) {
                    LOG.error(e.getLocalizedMessage(), e);
                }
            }
        } catch (EnvironmentStartException e) {
            environmentStartError = "Start of environment " + envName + " failed. Error: " + e.getLocalizedMessage();
        }

        publishEvent(EventType.ERROR,
                     workspaceId,
                     environmentStartError);

        // todo should we send STOPPED event as well?

        throw new ServerException(environmentStartError);
    }

    /**
     * This method is similar to the {@link #start(WorkspaceImpl, String, boolean)} method
     * except that it doesn't recover workspace and always starts a new one.
     */
    public RuntimeDescriptor start(WorkspaceImpl workspace, String envName) throws ServerException,
                                                                                   ConflictException,
                                                                                   NotFoundException {
        return start(workspace, envName, false);
    }

    /**
     * Stops running workspace runtime.
     *
     * <p>Stops environment in an implementation specific way.
     * During the stop of the workspace its runtime is accessible with {@link WorkspaceStatus#STOPPING stopping} status.
     * Workspace may be stopped only if its status is {@link WorkspaceStatus#RUNNING}.
     *
     * @param workspaceId
     *         identifier of workspace which should be stopped
     * @throws NotFoundException
     *         when workspace with specified identifier is not running
     * @throws ServerException
     *         when any error occurs during workspace stopping
     * @throws ConflictException
     *         when running workspace status is different from {@link WorkspaceStatus#RUNNING}
     * @see EnvironmentEngine#stop(String)
     * @see WorkspaceStatus#STOPPING
     */
    public void stop(String workspaceId) throws NotFoundException, ServerException, ConflictException {
        // This check allows to exit with an appropriate exception before blocking on lock.
        // The double check is required as it is still possible to get unlucky timing
        // between locking and stopping workspace.
        ensurePreDestroyIsNotExecuted();
        final WorkspaceRuntimeImpl runtime;
        try (StrippedLocks.WriteLock lock = strippedLocks.acquireWriteLock(workspaceId)) {
            ensurePreDestroyIsNotExecuted();
            final RuntimeDescriptor descriptor = descriptors.get(workspaceId);
            if (descriptor == null) {
                throw new NotFoundException("Workspace with id '" + workspaceId + "' is not running.");
            }
            if (descriptor.getRuntimeStatus() != WorkspaceStatus.RUNNING) {
                throw new ConflictException(
                        format("Couldn't stop '%s' workspace because its status is '%s'. Workspace can be stopped only if it is 'RUNNING'",
                               workspaceId,
                               descriptor.getRuntimeStatus()));
            }

            descriptor.setRuntimeStatus(WorkspaceStatus.STOPPING);

            // Create deep  copy of the currently running workspace to prevent
            // out of the lock instance modifications and stale data effects
            runtime = new WorkspaceRuntimeImpl(descriptor.getRuntime());
        }

        EnvironmentEngine engine = environmentEngines.get(runtime.getEnvType());
        if (engine == null) {
            throw new ServerException(String.format("Can't stop workspace %s. Engine of type %s not found",
                                                    workspaceId,
                                                    runtime.getEnvType()));
        }

        publishEvent(EventType.STOPPING, workspaceId, null);
        try {
            engine.stop(workspaceId);
            publishEvent(EventType.STOPPED, workspaceId, null);
        } catch (ServerException | RuntimeException e) {
            publishEvent(EventType.ERROR, workspaceId, e.getLocalizedMessage());
            // todo should we send STOPPED event as well?
        } finally {
            try (StrippedLocks.WriteLock lock = strippedLocks.acquireWriteLock(workspaceId)) {
                descriptors.remove(workspaceId);
            }
        }
    }

    /**
     * Returns true if workspace was started and its status is
     * {@link WorkspaceStatus#RUNNING running}, {@link WorkspaceStatus#STARTING starting}
     * or {@link WorkspaceStatus#STOPPING stopping} - otherwise returns false.
     *
     * <p> This method is less expensive alternative to {@link #get(String)} + {@code try catch}, see example:
     * <pre>{@code
     *
     *     if (!runtimes.hasRuntime("workspace123")) {
     *         doStuff("workspace123");
     *     }
     *
     *     //vs
     *
     *     try {
     *         runtimes.get("workspace123");
     *     } catch (NotFoundException ex) {
     *         doStuff("workspace123");
     *     }
     *
     * }</pre>
     *
     * @param workspaceId
     *         workspace identifier to perform check
     * @return true if workspace is running, otherwise false
     */
    public boolean hasRuntime(String workspaceId) {
        try (StrippedLocks.ReadLock lock = strippedLocks.acquireReadLock(workspaceId)) {
            return descriptors.containsKey(workspaceId);
        }
    }

    public MessageConsumer<MachineLogMessage> getEnvironmentLogger(String workspaceId) throws ServerException {
        WebsocketMessageConsumer<MachineLogMessage> envMessageConsumer =
                new WebsocketMessageConsumer<>(format(ENVIRONMENT_OUTPUT_CHANNEL_TEMPLATE, workspaceId));
        return new AbstractMessageConsumer<MachineLogMessage>() {
            @Override
            public void consume(MachineLogMessage message) throws IOException {
                envMessageConsumer.consume(message);
            }
        };
    }

    // todo find usage ; is it needed?
    public LineConsumer getMachineLogger(String workspaceId, String machineName) throws ServerException {
        WebsocketMessageConsumer<MachineLogMessage> envMessageConsumer =
                new WebsocketMessageConsumer<>(format(ENVIRONMENT_OUTPUT_CHANNEL_TEMPLATE, workspaceId));
        return new AbstractLineConsumer() {
            @Override
            public void writeLine(String line) throws IOException {
                envMessageConsumer.consume(new MachineLogMessageImpl(machineName, line));
            }
        };
    }

    @PostConstruct
    private void subscribe() {
        eventService.subscribe(removeMachineEventSubscriber);
    }

    /**
     * Removes all descriptors from the in-memory storage, while
     * {@link EnvironmentEngine} is responsible for environment destroying.
     */
    @PreDestroy
    @VisibleForTesting
    void cleanup() {
        isPreDestroyInvoked = true;

        // Unsubscribe from events
        eventService.unsubscribe(removeMachineEventSubscriber);

        final ExecutorService stopEnvExecutor =
                Executors.newFixedThreadPool(2 * Runtime.getRuntime().availableProcessors(),
                                             new ThreadFactoryBuilder().setNameFormat("StopEnvironment-%d")
                                                                       .setDaemon(false)
                                                                       .build());
        try (StrippedLocks.WriteAllLock lock = strippedLocks.acquireWriteAllLock()) {
            for (Map.Entry<String, RuntimeDescriptor> descriptorEntry : descriptors.entrySet()) {
                if (descriptorEntry.getValue().getRuntimeStatus().equals(WorkspaceStatus.RUNNING) ||
                    descriptorEntry.getValue().getRuntimeStatus().equals(WorkspaceStatus.STARTING)) {
                    stopEnvExecutor.execute(() -> {
                        try {
                            environmentEngines.get(descriptorEntry.getValue()
                                                                  .getRuntime()
                                                                  .getEnvType())
                                              .stop(descriptorEntry.getKey());
                        } catch (ServerException e) {
                            LOG.error(e.getLocalizedMessage(), e);
                        }
                    });
                }
            }

            descriptors.clear();

            stopEnvExecutor.shutdown();
        }
        try {
            if (!stopEnvExecutor.awaitTermination(50, TimeUnit.SECONDS)) {
                stopEnvExecutor.shutdownNow();
                if (!stopEnvExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                    LOG.warn("Unable terminate destroy machines pool");
                }
            }
        } catch (InterruptedException e) {
            stopEnvExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    @VisibleForTesting
    void publishEvent(EventType type, String workspaceId, String error) {
        eventService.publish(newDto(WorkspaceStatusEvent.class)
                                     .withEventType(type)
                                     .withWorkspaceId(workspaceId)
                                     .withError(error));
    }

    private void ensurePreDestroyIsNotExecuted() throws ServerException {
        if (isPreDestroyInvoked) {
            throw new ServerException("Could not perform operation because application server is stopping");
        }
    }

    /**
     * Wrapper for the {@link WorkspaceRuntime} instance.
     * Knows the state of the started workspace runtime,
     * helps to postpone {@code WorkspaceRuntime} instance creation to
     * the time when all the machines from the workspace are created.
     */
    public static class RuntimeDescriptor {

        private WorkspaceRuntimeImpl runtime;
        private WorkspaceStatus      status;

        private RuntimeDescriptor(WorkspaceRuntimeImpl runtime) {
            this.runtime = runtime;
        }

        private RuntimeDescriptor(RuntimeDescriptor descriptor) {
            this(new WorkspaceRuntimeImpl(descriptor.runtime));
            this.status = descriptor.status;
        }

        /** Returns the instance of {@code WorkspaceRuntime} described by this descriptor. */
        public WorkspaceRuntimeImpl getRuntime() {
            return runtime;
        }

        /**
         * Returns the status of the {@code WorkspaceRuntime} described by this descriptor.
         * Never returns {@link WorkspaceStatus#STOPPED} status, you'll rather get {@link NotFoundException}
         * from {@link #get(String)} method.
         */
        public WorkspaceStatus getRuntimeStatus() {
            return status;
        }

        private void setRuntimeStatus(WorkspaceStatus status) {
            this.status = status;
        }
    }

    // todo consider usage of listener added to engines. That listener can react on unpredictable stop of machines
    @VisibleForTesting
    class RemoveMachineEventSubscriber implements EventSubscriber<MachineStatusEvent> {

        @Override
        public void onEvent(MachineStatusEvent event) {
            // This event subscriber doesn't handle dev-machine destroyed events
            // as in that case workspace should be stopped, and stop should be asynchronous
            // but WorkspaceRuntimes provides only synchronous operations.
            if (event.getEventType() == MachineStatusEvent.EventType.DESTROYED && !event.isDev()) {
                removeMachine(event.getMachineId(),
                              event.getMachineName(),
                              event.getWorkspaceId());
            }
        }
    }

    /** Removes machine from the workspace runtime. */
    @VisibleForTesting
    void removeMachine(String machineId, String machineName, String workspaceId) {
        try (StrippedLocks.WriteLock lock = strippedLocks.acquireWriteLock(workspaceId)) {
            final RuntimeDescriptor descriptor = descriptors.get(workspaceId);

            // Machine can be removed only from existing runtime with 'RUNNING' status
            if (descriptor == null || descriptor.getRuntimeStatus() != WorkspaceStatus.RUNNING) {
                return;
            }

            // Try to remove non-dev machine from the runtime machines list
            // It is unusual but still possible to get the state when such machine
            // doesn't exist, in this case an appropriate warning will be logged
            if (!descriptor.getRuntime()
                           .getMachines()
                           .removeIf(m -> m.getConfig().getName().equals(machineName))) {
                LOG.warn("An attempt to remove the machine '{}' from the workspace runtime '{}' failed. " +
                         "Workspace doesn't contain machine with name '{}'",
                         machineId,
                         workspaceId,
                         machineName);
            }
        }
    }
}
