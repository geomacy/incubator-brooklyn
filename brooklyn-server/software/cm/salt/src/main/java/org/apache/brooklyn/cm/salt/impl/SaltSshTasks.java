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
import java.util.Set;

import org.apache.brooklyn.api.mgmt.TaskAdaptable;
import org.apache.brooklyn.api.mgmt.TaskFactory;
import org.apache.brooklyn.core.effector.EffectorTasks;
import org.apache.brooklyn.core.effector.ssh.SshEffectorTasks;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.file.ArchiveTasks;
import org.apache.brooklyn.util.core.task.TaskBuilder;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.ssh.BashCommands;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.text.Strings;

import static org.apache.brooklyn.util.ssh.BashCommands.sudo;


public class SaltSshTasks {

    private static final String SALT_FORMULA_ROOT = "/srv/salt";

    private SaltSshTasks() {
        // Utility class
    }

    public static TaskFactory<?> installSalt(boolean force) {
        // TODO: ignore force?
        List<String> commands = MutableList.<String>builder()
            .add(BashCommands.commandToDownloadUrlAs("https://bootstrap.saltstack.com", "install_salt.sh"))
            .add(sudo("sh install_salt.sh"))
            .build();
        return SshEffectorTasks.ssh(commands).summary("install salt");
    }

    public static TaskFactory<?> configureForMasterlessOperation(boolean force) {
        // TODO: ignore force?
        List<String> commands = MutableList.<String>builder()
            .add(BashCommands.installPackage("sed")) // hardly likely to be necessary but just in case...
            .add(sudo("sed -i '/^#file_client/c file_client: local' /etc/salt/minion"))
            .build();
        return SshEffectorTasks.ssh(commands).summary("configure masterless");
    }


    public static TaskFactory<?> enableFileRoots(boolean force) {

        List<String> commandLines = MutableList.<String>builder()
            .add("chmod go+w /etc/salt/minion")
            .add("grep ^file_roots /etc/salt/minion || cat >> /etc/salt/minion << BROOKLYN_EOF")
            .add("file_roots:")
            .add("  base:")
            .add("    - /srv/salt/")
            .add("BROOKLYN_EOF")
            .add(":") // required for sudo
            .build();
        return SshEffectorTasks.ssh(sudo(Strings.join(commandLines, "\n"))).summary("enable file_roots");
    }

    public static TaskFactory<?> installSaltFormula(final String formula, final String formulaUrl, boolean force) {
        return new TaskFactory<TaskAdaptable<?>>() {
            @Override
            public TaskAdaptable<?> newTask() {
                TaskBuilder<Void> tb = Tasks.<Void>builder().displayName("install formula " + formula);

                String tempDirectoryForUnpack = SALT_FORMULA_ROOT + "/tmp-" + Identifiers.makeRandomId(12);

                tb.add(ArchiveTasks.deploy(null, null, formulaUrl, EffectorTasks.findSshMachine(),
                    tempDirectoryForUnpack, false, null, null).newTask());

                return tb.build();
            }
        };
    }

    public static TaskFactory<?> installTopFile(final Set<? extends String> runList, boolean force) {
        // TODO: ignore force?

        final MutableList.Builder<String> topBuilder = MutableList.<String>builder()
            .add("cat > /srv/salt/top.sls << BROOKLYN_EOF")
            .add("base:")
            .add("  '*':");
        for (String stateName: runList) {
            topBuilder.add("    - " + stateName);
        }
        topBuilder.add("BROOKLYN_EOF");
        topBuilder.add(":"); // required for sudo to work
        List<String> topLines = topBuilder.build();

        List<String> commands = MutableList.<String>builder()
            .add(sudo("mkdir -p /srv/salt"))
            .add(sudo("chmod go+w /srv/salt"))
            .add(sudo(Strings.join(topLines, "\n")))
            .build();
        return SshEffectorTasks.ssh(commands).summary("create top.sls file");

    }
}
