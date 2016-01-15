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
    
    public static final TaskFactory<?> installSalt(boolean force) {
        // TODO: ignore force?
        List<String> commands = MutableList.<String>builder()
            .add(BashCommands.commandToDownloadUrlAs("https://bootstrap.saltstack.com", "install_salt.sh"))
            .add(BashCommands.sudo("sh install_salt.sh"))
            .build();
        return SshEffectorTasks.ssh(commands).summary("install salt");
    }

    public static final TaskFactory<?> configureForMasterlessOperation(boolean force) {
        // TODO: ignore force?
        List<String> commands = MutableList.<String>builder()
            .add(BashCommands.installPackage("sed")) // hardly likely to be necessary but just in case...
            .add(BashCommands.sudo("sed -i '/^#file_client/a file_client: local' /etc/salt/minion"))
            .build();
        return SshEffectorTasks.ssh(commands).summary("configure masterless");
    }

    public static final TaskFactory<?> installSaltFormulas(final Map<String, Object> formulas, boolean force) {
        return SshEffectorTasks.ssh(BashCommands.INSTALL_CURL).summary("install salt-ssh");
    }

}
