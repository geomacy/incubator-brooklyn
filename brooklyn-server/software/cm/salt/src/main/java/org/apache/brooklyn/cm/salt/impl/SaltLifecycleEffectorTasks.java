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

import java.util.Map;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.cm.salt.SaltConfig;
import org.apache.brooklyn.entity.software.base.SoftwareProcess;
import org.apache.brooklyn.entity.software.base.lifecycle.MachineLifecycleEffectorTasks;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.DynamicTasks;
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
        LOG.info("Starting salt in '{}' mode at '{}'", mode, machineS.get().getDisplayName());
        if (mode == SaltMode.MASTERLESS) {
            startWithSshAsync();
        } else {
            // TODO: implement MASTER and MINION
            throw new IllegalStateException("Unknown salt mode: " + mode.name());
        }
        return "salt tasks submitted (" + mode + ")";
    }

    @Override
    protected String stopProcessesAtMachine() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected StopMachineDetails<Integer> stopAnyProvisionedMachines() {
        return super.stopAnyProvisionedMachines();
    }

    protected static SaltMode detectSaltMode(Entity entity) {
        SaltMode mode = entity.getConfig(SaltConfig.SALT_MODE);
        Preconditions.checkNotNull(mode, "Required confg " + SaltConfig.SALT_MODE + " not provided for entity: " + entity);
        return mode;
    }

    protected void startWithSshAsync() {
        LOG.debug("");
        
        Map<String, Object> formulas = ConfigBag.newInstance(entity()
            .getConfig(SaltConfig.SALT_FORMULAS))
            .getAllConfig();
        DynamicTasks.queue(
            SaltSshTasks.installSaltSsh(false),
            SaltSshTasks.installSaltFormulas(formulas, false));
        
        // TODO: actually installing service
    }

    protected void postStartCustom() {
        // TODO: check for package installed?
        entity().sensors().set(SoftwareProcess.SERVICE_UP, true);
    }
}
