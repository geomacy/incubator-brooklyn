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

import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.mgmt.TaskFactory;
import org.apache.brooklyn.core.effector.ssh.SshEffectorTasks;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.ssh.BashCommands;


public class SaltSshTasks {

    private SaltSshTasks() {
        // Utility class
    }
    
    public static final TaskFactory<?> installSaltSsh(boolean force) {
        // TODO: ignore force?
        List<String> commands = MutableList.<String>builder()
            .add(BashCommands.ifExecutableElse0("apt-get", BashCommands.chain(
                BashCommands.sudo("add-apt-repository -y ppa:saltstack/salt"))))
            .add(BashCommands.installPackage(MutableMap.of("yum", "salt-ssh", "apt", "salt-ssh"), null))
            .build();
        return SshEffectorTasks.ssh(commands).summary("install salt-ssh");
    }

    public static final TaskFactory<?> installSaltFormulas(final Map<String, Object> formulas, boolean force) {
        return SshEffectorTasks.ssh(BashCommands.INSTALL_CURL).summary("install salt-ssh");
    }

}
