/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.brooklyn.cm.salt.impl;

import java.util.Set;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.mgmt.TaskAdaptable;
import org.apache.brooklyn.cm.salt.SaltConfig;
import org.apache.brooklyn.entity.software.base.SoftwareProcess;
import org.apache.brooklyn.entity.software.base.lifecycle.MachineLifecycleEffectorTasks;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.TaskBuilder;
import org.apache.brooklyn.util.core.task.system.ProcessTaskWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;

@Beta
public class SaltLifecycleEffectorTasks extends MachineLifecycleEffectorTasks implements SaltConfig {
    private static final Logger LOG = LoggerFactory.getLogger(SaltLifecycleEffectorTasks.class);

    @Override
    protected String startProcessesAtMachine(Supplier<MachineLocation> machineS) {
        SaltMode mode = detectSaltMode(entity());
        final MachineLocation machine = machineS.get();
        LOG.info("Starting salt in '{}' mode at '{}'", mode, machine.getDisplayName());
        if (mode == SaltMode.MASTERLESS) {
            startWithSshAsync();
        } else {
            // TODO: implement MASTER and MINION
            throw new IllegalStateException("Unknown salt mode: " + mode.name());
        }
        return "salt tasks submitted (" + mode + ")";
    }


    protected static SaltMode detectSaltMode(Entity entity) {
        SaltMode mode = entity.getConfig(SaltConfig.SALT_MODE);
        Preconditions.checkNotNull(mode, "Required config " + SaltConfig.SALT_MODE + " not provided for entity: " + entity);
        return mode;
    }

    protected void startWithSshAsync() {

        final Set<? extends String> startStates = entity().getConfig(SaltConfig.START_STATES);

        final Set<? extends String> formulas = entity()
            .getConfig(SaltConfig.SALT_FORMULAS);

        DynamicTasks.queue(
            SaltSshTasks.installSalt(false),
            SaltSshTasks.installSaltUtilities(false),
            SaltSshTasks.configureForMasterlessOperation(false),
            SaltSshTasks.installTopFile(startStates, false));

        if (formulas.size() > 0) {
            DynamicTasks.queue(SaltSshTasks.enableFileRoots(false));

            final TaskBuilder<Object> formulaTasks = TaskBuilder.builder().displayName("installing formulas");
            for (String url : formulas) {
                formulaTasks.add(SaltSshTasks.installSaltFormula(url, false).newTask());
            }
            DynamicTasks.queue(formulaTasks.build());
        }

        final TaskAdaptable applyState = SaltSshTasks.applyTopState(false);
        DynamicTasks.queue(applyState);
        applyState.asTask().blockUntilEnded();

        connectSensors();

    }

    private void connectSensors() {
        final ProcessTaskWrapper<String> retrieveHighstate = SaltSshTasks.retrieveHighstate();
        final ProcessTaskWrapper<String> highstate = DynamicTasks.queue(retrieveHighstate).block();
        String stateDescription = highstate.get();

        SaltHighstate.applyHighstate(stateDescription, entity());
    }


    protected void postStartCustom() {
        // TODO: check for package installed?
        entity().sensors().set(SoftwareProcess.SERVICE_UP, true);
    }


    @Override
    protected String stopProcessesAtMachine() {
        final Set<? extends String> stopStates = entity().getConfig(SaltConfig.STOP_STATES);
        LOG.debug("Executing Salt stopProcessesAtMachine with states {}", stopStates);
        if (stopStates.isEmpty()) {
            stopBasedOnStartStates();
        } else {
            applyStates(stopStates);
        }
        return null;
    }

    private void applyStates(Set<? extends String> stopStates) {
        for (String state : stopStates) {
            DynamicTasks.queue(SaltSshTasks.applyState(state, false));
        }

    }

    private void stopBasedOnStartStates() {
        final Set<? extends String> startStates = entity().getConfig(SaltConfig.START_STATES);
        DynamicTasks.queue(SaltSshTasks.stopFromStates(startStates, false));
    }

    public void restart(ConfigBag parameters) {
        final Set<? extends String> restartStates = entity().getConfig(SaltConfig.RESTART_STATES);
        LOG.debug("Executing Salt stopProcessesAtMachine with states {}", restartStates);
        if (restartStates.isEmpty()) {
            restartBasedOnStartStates();
        } else {
            applyStates(restartStates);
        }
    }

    private void restartBasedOnStartStates() {
        // TODO
    }
}
