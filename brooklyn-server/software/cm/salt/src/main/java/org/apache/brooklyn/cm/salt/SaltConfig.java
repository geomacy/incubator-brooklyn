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
package org.apache.brooklyn.cm.salt;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.MapConfigKey;
import org.apache.brooklyn.core.config.SetConfigKey;
import org.apache.brooklyn.util.core.flags.SetFromFlag;

import com.google.common.annotations.Beta;

/**
 * {@link ConfigKey}s used to configure Salt entities.
 *
 * @see SaltConfigs
 */
@Beta
public interface SaltConfig {

    enum SaltMode {
        /** Master entity */
        MASTER, 
        /** Minion entity */
        MINION,
        /** Masterless entity using salt-ssh */
        MASTERLESS
    }

    @SetFromFlag("salt.mode")
    ConfigKey<SaltMode> SALT_MODE = ConfigKeys.newConfigKey(SaltMode.class, "brooklyn.salt.mode",
            "SaltStack execution mode (master/minion/masterless)", SaltMode.MASTERLESS);

    @SetFromFlag("formulas")
    MapConfigKey<String> SALT_FORMULAS = new MapConfigKey<>(String.class, "brooklyn.salt.formulaUrls",
            "Map of Salt formula URLs", ImmutableMap.<String,String>of());

    @SetFromFlag("runList")
    SetConfigKey<String> SALT_RUN_LIST = new SetConfigKey<>(String.class, "brooklyn.salt.runList",
            "Set of Salt states to apply", ImmutableSet.<String>of());

    MapConfigKey<Object> SALT_SSH_LAUNCH_ATTRIBUTES = new MapConfigKey<>(Object.class, "brooklyn.salt.ssh.launch.attributes", "TODO");

}
