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
package org.eclipse.che.ide.projectimport.wizard;

import com.google.inject.Inject;
import com.google.inject.Singleton;

import org.eclipse.che.api.project.shared.Constants;
import org.eclipse.che.api.project.shared.dto.SourceEstimation;
import org.eclipse.che.api.promises.client.Function;
import org.eclipse.che.api.promises.client.FunctionException;
import org.eclipse.che.api.promises.client.Promise;
import org.eclipse.che.api.promises.client.PromiseProvider;
import org.eclipse.che.ide.api.project.MutableProjectConfig;
import org.eclipse.che.ide.api.project.type.ProjectTypeRegistry;
import org.eclipse.che.ide.api.resources.Project;
import org.eclipse.che.ide.projecttype.wizard.presenter.ProjectWizardPresenter;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;

/**
 * The class contains business logic which allows resolve project type and call updater.
 *
 * @author Dmitry Shnurenko
 * @author Vlad Zhukovskyi
 */
@Singleton
public class ProjectResolver {

    private final ProjectTypeRegistry    projectTypeRegistry;
    private final PromiseProvider        promiseProvider;
    private final ProjectWizardPresenter projectWizard;

    @Inject
    public ProjectResolver(ProjectTypeRegistry projectTypeRegistry,
                           PromiseProvider promiseProvider,
                           ProjectWizardPresenter projectWizard) {
        this.projectTypeRegistry = projectTypeRegistry;
        this.promiseProvider = promiseProvider;
        this.projectWizard = projectWizard;
    }

    public Promise<Project> resolve(final Project project) {
        return project.resolve().thenPromise(new Function<List<SourceEstimation>, Promise<Project>>() {
            @Override
            public Promise<Project> apply(List<SourceEstimation> estimations) throws FunctionException {
                if (estimations == null || estimations.isEmpty()) {
                    return promiseProvider.resolve(project);
                }

                final List<String> primeTypes = newArrayList();
                for (SourceEstimation estimation : estimations) {
                    if (projectTypeRegistry.getProjectType(estimation.getType()).isPrimaryable()) {
                        primeTypes.add(estimation.getType());
                    }
                }

                final MutableProjectConfig config = new MutableProjectConfig(project);

                if (primeTypes.isEmpty()) {
                    return promiseProvider.resolve(project);
                } else if (primeTypes.size() == 1) {
                    config.setType(primeTypes.get(0));
                } else {
                    config.setType(Constants.BLANK_ID);
                    projectWizard.show(config);

                    return promiseProvider.resolve(project);
                }

                return project.update().withBody(config).send();
            }
        });
    }
}
