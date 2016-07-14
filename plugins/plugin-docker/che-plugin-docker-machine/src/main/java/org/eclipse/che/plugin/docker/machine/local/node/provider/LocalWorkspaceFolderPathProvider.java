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
package org.eclipse.che.plugin.docker.machine.local.node.provider;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.assistedinject.Assisted;

import org.eclipse.che.api.core.NotFoundException;
import org.eclipse.che.api.core.ServerException;
import org.eclipse.che.api.core.model.workspace.Workspace;
import org.eclipse.che.api.core.util.SystemInfo;
import org.eclipse.che.api.workspace.server.WorkspaceManager;
import org.eclipse.che.plugin.docker.machine.WindowsHostUtils;
import org.eclipse.che.plugin.docker.machine.node.WorkspaceFolderPathProvider;

import javax.annotation.PostConstruct;
import javax.inject.Named;
import javax.inject.Singleton;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Provides path to workspace folder in CHE.
 *
 * @author Alexander Garagatyi
 */
@Singleton
public class LocalWorkspaceFolderPathProvider implements WorkspaceFolderPathProvider {

    /**
     * Value provide path to directory on host machine where will by all created and mount to the
     * created workspaces folder that become root of workspace inside machine.
     * Inside machine it will point to the directory described by {@literal che.machine.projects.internal.storage}.
     * <p>
     * For example:
     * if you set {@literal che.user.workspaces.storage} to the /home/user/che/workspaces after creating new workspace will be created new folder
     * /home/user/che/workspaces/{workspaceName} and it will be mount to the  dev-machine to {@literal che.machine.projects.internal.storage}
     */
    private final String                     workspacesMountPoint;
    private final Provider<WorkspaceManager> workspaceManager;
    /**
     * this value provide path to projects on local host
     * if this value will be set all workspace will manage
     * same projects from your host
     */
    @Inject(optional = true)
    @Named("host.projects.root")
    private String hostProjectsFolder;

    @Inject(optional = true)
    @Named("che.user.workspaces.storage.create_folders")
    private boolean createFolders = true;

    @Inject
    public LocalWorkspaceFolderPathProvider(@Named("che.user.workspaces.storage") String workspacesMountPoint,
                                            Provider<WorkspaceManager> workspaceManager) throws IOException {
        this.workspacesMountPoint = getWorkspacesRootPath(workspacesMountPoint);
        this.workspaceManager = workspaceManager;
    }

    @VisibleForTesting
    protected LocalWorkspaceFolderPathProvider(String workspacesMountPoint,
                                               String projectsFolder,
                                               Provider<WorkspaceManager> workspaceManager,
                                               boolean createFolders) throws IOException {
        this.workspaceManager = workspaceManager;
        this.workspacesMountPoint = getWorkspacesRootPath(workspacesMountPoint);
        this.hostProjectsFolder = projectsFolder;
        this.createFolders = createFolders;
    }

    @Override
    public String getPath(@Assisted("workspace") String workspaceId) throws IOException {
        if (hostProjectsFolder != null) {
            return hostProjectsFolder;
        } else {
            try {
                WorkspaceManager workspaceManager = this.workspaceManager.get();
                final Workspace workspace = workspaceManager.getWorkspace(workspaceId);
                String wsName = workspace.getConfig().getName();
                Path folder = Paths.get(workspacesMountPoint).resolve(wsName);
                if (Files.notExists(folder)) {
                    Files.createDirectory(folder);
                }
                return folder.toString();
            } catch (NotFoundException | ServerException e) {
                throw new IOException(e.getMessage());
            }
        }
    }

    @VisibleForTesting
    @PostConstruct
    void createNeededFolders() throws IOException {
        if (SystemInfo.isWindows()) {
            final Path cheHome = WindowsHostUtils.ensureCheHomeExist(createFolders);
            final Path vfs = cheHome.resolve("vfs");
            if (createFolders) {
                if (!vfs.toFile().mkdir()) {
                    throw new IOException(String.format("Can't create folder %s for projects of workspaces", vfs));
                }
            }
        }

        if (workspacesMountPoint != null && createFolders) {
            ensureExist(workspacesMountPoint, "che.user.workspaces.storage");
        }
        if (hostProjectsFolder != null && createFolders) {
            ensureExist(hostProjectsFolder, "host.projects.root");
        }
    }

    private String getWorkspacesRootPath(String configuredPath) throws IOException {
        if (SystemInfo.isWindows()) {
            Path cheHome = WindowsHostUtils.getCheHome();
            return cheHome.resolve("vfs").toString();
        }
        return configuredPath;
    }

    private void ensureExist(String path, String prop) throws IOException {
        Path folder = Paths.get(path);
        if (Files.notExists(folder)) {
            Files.createDirectory(folder);
        }
        if (!Files.isDirectory(folder)) {
            throw new IOException(String.format("Projects %s is not directory. Check %s configuration property.", path, prop));
        }
    }
}
