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
package org.eclipse.che.api.environment.impl.che;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.eclipse.che.api.core.BadRequestException;
import org.eclipse.che.api.core.ConflictException;
import org.eclipse.che.api.core.NotFoundException;
import org.eclipse.che.api.core.ServerException;
import org.eclipse.che.api.core.model.machine.Machine;
import org.eclipse.che.api.core.model.machine.MachineConfig;
import org.eclipse.che.api.core.model.machine.MachineLogMessage;
import org.eclipse.che.api.core.model.machine.MachineStatus;
import org.eclipse.che.api.core.model.workspace.Environment;
import org.eclipse.che.api.core.util.AbstractLineConsumer;
import org.eclipse.che.api.core.util.LineConsumer;
import org.eclipse.che.api.core.util.MessageConsumer;
import org.eclipse.che.api.environment.server.EnvironmentStartException;
import org.eclipse.che.api.environment.server.spi.EnvironmentEngine;
import org.eclipse.che.api.machine.server.MachineManager;
import org.eclipse.che.api.machine.server.exception.MachineException;
import org.eclipse.che.api.machine.server.model.impl.MachineConfigImpl;
import org.eclipse.che.api.machine.server.model.impl.MachineImpl;
import org.eclipse.che.api.machine.server.model.impl.MachineLogMessageImpl;
import org.eclipse.che.api.workspace.server.StrippedLocks;
import org.slf4j.Logger;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * @author Alexander Garagatyi
 */
// todo consider addition of version to environment
public class CheEnvironmentEngine implements EnvironmentEngine {

    private static final Logger LOG              = getLogger(CheEnvironmentEngine.class);
    public static final  String ENVIRONMENT_TYPE = "che";

    private final MachineManager                 machineManager;
    private final Map<String, EnvironmentHolder> environments;
    private final StrippedLocks                  strippedLocks;

    private volatile boolean isPreDestroyInvoked;

    @Inject
    public CheEnvironmentEngine(MachineManager machineManager,
                                CheEnvironmentValidator cheEnvironmentValidator) {
        this.machineManager = machineManager;
        this.environments = new HashMap<>();
        // 16 - experimental value for stripes count, it comes from default hash map size
        this.strippedLocks = new StrippedLocks(16);
    }

    @Override
    public String getType() {
        return ENVIRONMENT_TYPE;
    }

    @Override
    public List<Machine> start(String workspaceId,
                               Environment env,
                               boolean recover,
                               MessageConsumer<MachineLogMessage> messageConsumer) throws EnvironmentStartException,
                                                                                          ServerException {
        // Create a new start queue with a dev machine in the queue head
        List<MachineConfigImpl> startConfigs = env.getMachineConfigs()
                                                  .stream()
                                                  .map(MachineConfigImpl::new)
                                                  .collect(Collectors.toList());
        final MachineConfigImpl devCfg = removeFirstMatching(startConfigs, MachineConfig::isDev);
        startConfigs.add(0, devCfg);

        EnvironmentHolder environmentHolder = new EnvironmentHolder(null,
                                                                    null,
                                                                    messageConsumer);
        environmentHolder.startQueue = new ArrayDeque<>(startConfigs);
        environmentHolder.machines = new ArrayList<>();

        try (StrippedLocks.WriteLock lock = strippedLocks.acquireWriteLock(workspaceId)) {
            environments.put(workspaceId, environmentHolder);
        }

        startQueue(workspaceId, env.getName(), recover, messageConsumer);

        List<MachineImpl> machines;
        try (StrippedLocks.ReadLock lock = strippedLocks.acquireReadLock(workspaceId)) {
            machines = environments.get(workspaceId).machines;
        }
        return new ArrayList<>(machines);
    }

    @Override
    public void stop(String workspaceId) throws ServerException, NotFoundException {
        // At this point of time starting queue must be removed
        // to prevent start of another machines which are not started yet.
        // In this case workspace start will be interrupted and
        // interruption will be reported, machine which is currently starting(if such exists)
        // will be destroyed by workspace starting thread.
        List<MachineImpl> machinesCopy = null;
        try (StrippedLocks.WriteLock lock = strippedLocks.acquireWriteLock(workspaceId)) {
            EnvironmentHolder environmentHolder = environments.get(workspaceId);
            environmentHolder.startQueue = null;
            List<MachineImpl> machines = environmentHolder.machines;
            if (machines != null && !machines.isEmpty()) {
                machinesCopy = new ArrayList<>(machines);
            }
        }

        if (machinesCopy != null) {
            destroyRuntime(workspaceId, machinesCopy);
        }
    }

    private void startQueue(String workspaceId,
                            String envName,
                            boolean recover,
                            MessageConsumer<MachineLogMessage> messageConsumer) throws EnvironmentStartException,
                                                                                       ServerException {
        // Starting all the machines one by one by getting configs
        // from the corresponding starting queue.
        // Config will be null only if there are no machines left in the queue
        MachineConfigImpl config = queuePeekOrFail(workspaceId);
        while (config != null) {
            // todo should we support stop of env on dev machine failure on that level?
            // According to WorkspaceStatus specification the workspace start
            // is failed when dev-machine start is failed, so if any error
            // occurs during machine creation and the machine is dev-machine
            // then start fail is reported and start resources such as queue
            // and descriptor must be cleaned up
            MachineImpl machine = null;
            try {
                machine = startMachine(config,
                                       workspaceId,
                                       envName,
                                       recover,
                                       messageConsumer);
            } catch (RuntimeException | ServerException | ConflictException | NotFoundException x) {
                if (config.isDev()) {
                    cleanupStartResources(workspaceId);
                    throw x;
                }
                LOG.error(format("Error while creating non-dev machine '%s' in workspace '%s', environment '%s'",
                                 config.getName(),
                                 workspaceId,
                                 envName),
                          x);
            }

            // Machine destroying is an expensive operation which must be
            // performed outside of the lock, this section checks if
            // the workspace wasn't stopped while it is starting and sets
            // polled flag to true if the workspace wasn't stopped plus
            // polls the proceeded machine configuration from the queue
            boolean queuePolled = false;
            try (StrippedLocks.WriteLock lock = strippedLocks.acquireWriteLock(workspaceId)) {
                ensurePreDestroyIsNotExecuted();
                EnvironmentHolder environmentHolder = environments.get(workspaceId);
                final Queue<MachineConfigImpl> queue = environmentHolder.startQueue;
                if (queue != null) {
                    queue.poll();
                    queuePolled = true;
                    if (machine != null) {
                        environmentHolder.machines.add(machine);
                    }
                }
            }

            // If machine config is not polled from the queue
            // then workspace was stopped and newly created machine
            // must be destroyed(if such exists)
            if (!queuePolled) {
                if (machine != null) {
                    machineManager.destroy(machine.getId(), false);
                }
                throw new EnvironmentStartException(
                        format("Workspace '%s' start interrupted. Workspace stopped before all its machines started",
                               workspaceId));
            }

            config = queuePeekOrFail(workspaceId);
        }

        // All the machines tried to start which means that queue
        // should be empty and can be normally removed, but in the case of
        // some unlucky timing, the workspace may be stopped and started again
        // so the queue, which is guarded by the same lock as workspace descriptor
        // may be initialized again with a new batch of machines to start,
        // that's why queue should be removed only if it is empty.
        // On the other hand queue may not exist because workspace has been stopped
        // just before queue utilization, which considered as a normal behaviour
        try (StrippedLocks.WriteLock lock = strippedLocks.acquireWriteLock(workspaceId)) {
            EnvironmentHolder environmentHolder = environments.get(workspaceId);
            final Queue<MachineConfigImpl> queue = environmentHolder.startQueue;
            if (queue != null && queue.isEmpty()) {
                environmentHolder.startQueue = null;
            }
        }
    }

    /**
     * Gets head config from the queue associated with the given {@code workspaceId}.
     *
     * <p>Note that this method won't actually poll the queue.
     *
     * <p>Fails if workspace start was interrupted by stop(queue doesn't exist).
     *
     * @return machine config which is in the queue head, or null
     * if there are no machine configs left
     * @throws EnvironmentStartException
     *         when queue doesn't exist which means that {@link #stop(String)} executed
     *         before all the machines started
     * @throws ServerException
     *         only if pre destroy has been invoked before peek config retrieved
     */
    private MachineConfigImpl queuePeekOrFail(String workspaceId) throws EnvironmentStartException,
                                                                         ServerException {
        try (StrippedLocks.ReadLock lock = strippedLocks.acquireReadLock(workspaceId)) {
            ensurePreDestroyIsNotExecuted();
            EnvironmentHolder environmentHolder = environments.get(workspaceId);
            if (environmentHolder == null || environmentHolder.startQueue == null) {
                throw new EnvironmentStartException(
                        format("Workspace '%s' start interrupted. Workspace was stopped before all its machines were started",
                               workspaceId));
            }
            return environmentHolder.startQueue.peek();
        }
    }

    /**
     * Starts the machine from the configuration, returns null if machine start failed.
     */
    private MachineImpl startMachine(MachineConfigImpl config,
                                     String workspaceId,
                                     String envName,
                                     boolean recover,
                                     MessageConsumer<MachineLogMessage> messageConsumer) throws ServerException,
                                                                                                NotFoundException,
                                                                                                ConflictException {
        LineConsumer machineLogConsumer = new AbstractLineConsumer() {
            @Override
            public void writeLine(String line) throws IOException {
                messageConsumer.consume(new MachineLogMessageImpl(config.getName(), line));
            }
        };

        MachineImpl machine;
        try {
            if (recover) {
                machine = machineManager.recoverMachine(config, workspaceId, envName, machineLogConsumer);
            } else {
                machine = machineManager.createMachineSync(config, workspaceId, envName, machineLogConsumer);
            }
        } catch (ConflictException x) {
            // The conflict is because of the already running machine
            // which may be running by several reasons:
            // 1. It has been running before the workspace started
            // 2. It was started immediately after the workspace
            // Consider the next example:
            // If workspace starts machines from configurations [m1, m2, m3]
            // and currently starting machine is 'm2' then it is still possible
            // to use direct Machine API call to start the machine 'm3'
            // which will result as a conflict for the workspace API during 'm3' start.
            // This is not usual/normal behaviour but it should be handled.
            // The handling logic gets the running machine instance by the 'm3' config
            // and considers that the machine is started correctly, if it is impossible
            // to find the corresponding machine then the fail will be reported
            // and workspace runtime state will be changed according to the machine config context.
            final Optional<MachineImpl> machineOpt = machineManager.getMachines()
                                                                   .stream()
                                                                   .filter(m -> m.getWorkspaceId().equals(workspaceId)
                                                                                && m.getEnvName().equals(envName)
                                                                                && m.getConfig().equals(config))
                                                                   .findAny();
            if (machineOpt.isPresent() && machineOpt.get().getStatus() == MachineStatus.RUNNING) {
                machine = machineOpt.get();
            } else {
                throw x;
            }
        } catch (BadRequestException x) {
            // TODO don't throw bad request exception from machine manager
            throw new IllegalArgumentException(x.getLocalizedMessage(), x);
        }
        return machine;
    }

    /**
     * Stops workspace by destroying all its machines and removing it from in memory storage.
     */
    private void destroyRuntime(String workspaceId, List<MachineImpl> machines) throws NotFoundException, ServerException {
        // Preparing the list of machines to be destroyed, dev machine goes last
        final MachineImpl devMachine = removeFirstMatching(machines, m -> m.getConfig().isDev());

        // Synchronously destroying all non-dev machines
        for (MachineImpl machine : machines) {
            try {
                machineManager.destroy(machine.getId(), false);
            } catch (NotFoundException ignore) {
                // This may happen, if machine is stopped by direct call to the Machine API
                // MachineManager cleanups all the machines due to application server shutdown
                // As non-dev machines don't affect runtime status, this exception is ignored
            } catch (RuntimeException | MachineException ex) {
                LOG.error(format("Could not destroy machine '%s' of workspace '%s'",
                                 machine.getId(),
                                 machine.getWorkspaceId()),
                          ex);
            }
        }

        // Synchronously destroying dev-machine
        try {
            machineManager.destroy(devMachine.getId(), false);
        } catch (NotFoundException ignore) {
            // This may happen, if machine is stopped by direct call to the Machine API
            // MachineManager cleanups all the machines due to application server shutdown
            // In this case workspace is considered as successfully stopped
        } finally {
            removeRuntime(workspaceId);
        }
    }

    @VisibleForTesting
    void cleanupStartResources(String workspaceId) {
        try (StrippedLocks.WriteLock lock = strippedLocks.acquireWriteLock(workspaceId)) {
            descriptors.remove(workspaceId);
        }
    }

    @VisibleForTesting
    void removeRuntime(String workspaceId) {
        try (StrippedLocks.WriteLock lock = strippedLocks.acquireWriteLock(workspaceId)) {
            environments.remove(workspaceId);
        }
    }

    /**
     * Removes all descriptors from the in-memory storage, while
     * {@link MachineManager#cleanup()} is responsible for machines destroying.
     */
    @PreDestroy
    @VisibleForTesting
    void cleanup() {
        isPreDestroyInvoked = true;
        final ExecutorService destroyMachinesExecutor =
                Executors.newFixedThreadPool(2 * Runtime.getRuntime().availableProcessors(),
                                             new ThreadFactoryBuilder().setNameFormat("DestroyMachine-%d")
                                                                       .setDaemon(false)
                                                                       .build());
        try (StrippedLocks.WriteAllLock lock = strippedLocks.acquireWriteAllLock()) {
            startQueues.clear();

            try {
                for (MachineImpl machine : machineManager.getMachines()) {
                    destroyMachinesExecutor.execute(() -> {
                        try {
                            machineManager.destroy(machine.getId(), false);
                        } catch (NotFoundException ignore) {
                            // it is ok, machine has been already destroyed
                        } catch (Exception e) {
                            LOG.warn(e.getLocalizedMessage());
                        }
                    });
                }
            } catch (MachineException e) {
                LOG.error(e.getLocalizedMessage(), e);
            }
            destroyMachinesExecutor.shutdown();
        }
        try {
            if (!destroyMachinesExecutor.awaitTermination(50, TimeUnit.SECONDS)) {
                destroyMachinesExecutor.shutdownNow();
                if (!destroyMachinesExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                    LOG.warn("Unable terminate destroy machines pool");
                }
            }
        } catch (InterruptedException e) {
            destroyMachinesExecutor.shutdownNow();
        }
    }

    private static <T> T removeFirstMatching(List<? extends T> elements, Predicate<T> predicate) {
        T element = null;
        for (final Iterator<? extends T> it = elements.iterator(); it.hasNext() && element == null; ) {
            final T next = it.next();
            if (predicate.test(next)) {
                element = next;
                it.remove();
            }
        }
        return element;
    }

    private void ensurePreDestroyIsNotExecuted() throws ServerException {
        if (isPreDestroyInvoked) {
            throw new ServerException("Could not perform operation because application server is stopping");
        }
    }

    private static class EnvironmentHolder {
        Queue<MachineConfigImpl>           startQueue;
        List<MachineImpl>                  machines;
        MessageConsumer<MachineLogMessage> outputConsumer;

        public EnvironmentHolder(Queue<MachineConfigImpl> startQueue,
                                 List<MachineImpl> machines,
                                 MessageConsumer<MachineLogMessage> outputConsumer) {
            this.startQueue = startQueue;
            this.machines = machines;
            this.outputConsumer = outputConsumer;
        }
    }
}
