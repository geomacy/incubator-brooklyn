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
import org.apache.brooklyn.util.core.task.system.ProcessTaskWrapper;
import org.apache.brooklyn.util.ssh.BashCommands;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.text.Strings;

import static org.apache.brooklyn.util.ssh.BashCommands.sudo;


public class SaltSshTasks {

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
            .add("grep ^file_roots /etc/salt/minion || {")
            .add("cat /etc/salt/minion > /tmp/minion.update")
            .add("cat >> /tmp/minion.update  << BROOKLYN_EOF")
            .add("file_roots:")
            .add("  base:")
            .add("    - /srv/salt/")
            .add("BROOKLYN_EOF")
            .add(sudo("mv /tmp/minion.update /etc/salt/minion"))
            .add("}")
            .build();
        return SshEffectorTasks.ssh(Strings.join(commandLines, "\n"))
            .requiringExitCodeZero()
            .summary("enable file_roots");
    }

    public static TaskFactory<?> installSaltFormula(final String formulaUrl, boolean force) {
        return new TaskFactory<TaskAdaptable<?>>() {
            @Override
            public TaskAdaptable<?> newTask() {
                TaskBuilder<Void> tb = Tasks.<Void>builder().displayName("install formula " + formulaUrl);

                String tempDirectoryForUnpack = "/tmp/download-" + Identifiers.makeRandomId(12);

                tb.add(ArchiveTasks.deploy(null, null, formulaUrl, EffectorTasks.findSshMachine(),
                    tempDirectoryForUnpack, false, null, null).newTask());

                String installCmd = BashCommands.chain(
                    "cd "+tempDirectoryForUnpack,
                    "EXPANDED_DIR=`ls`",
                    BashCommands.requireTest("`ls | wc -w` -eq 1",
                        "The deployed archive "+ formulaUrl +" must contain exactly one directory"),
                    "sudo mkdir -p /srv/formula",
                    "sudo mv $EXPANDED_DIR /srv/formula/",
                    // sed command below relies on enableFileRoots behaviour of append file_roots to end of config file
                    "sudo sed -i \"$ a\\    - /srv/formula/$EXPANDED_DIR\" /etc/salt/minion",
                    "cd ..",
                    "rm -rf '"+tempDirectoryForUnpack+"'");
                tb.add(SshEffectorTasks.ssh(installCmd).summary("installing " + formulaUrl + " states to /srv/formula")
                    .requiringExitCodeZero().newTask());

                return tb.build();
            }
        };
    }

    public static TaskFactory<?> installTopFile(final Set<? extends String> runList, boolean force) {
        // TODO: ignore force?

        final MutableList.Builder<String> topBuilder = MutableList.<String>builder()
            .add("cat > /tmp/top.sls << BROOKLYN_EOF")
            .add("base:")
            .add("  '*':");
        for (String stateName: runList) {
            topBuilder.add("    - " + stateName);
        }
        topBuilder.add("BROOKLYN_EOF");
        List<String> createTempTopFile = topBuilder.build();

        List<String> commands = MutableList.<String>builder()
            .add(sudo("mkdir -p /srv/salt"))
            .add(Strings.join(createTempTopFile, "\n"))
            .add(sudo("mv /tmp/top.sls /srv/salt"))
            .build();
        return SshEffectorTasks.ssh(commands).summary("create top.sls file");

    }

    public static TaskAdaptable applyTopState(boolean force) {
        return SshEffectorTasks.ssh(sudo("salt-call --local state.apply")).summary("salt state.apply").newTask();
    }

    public static TaskAdaptable applyState(String state, boolean force) {
        final String commandName = "state.apply " + state;
        return SshEffectorTasks.ssh(sudo("salt-call --local " + commandName)).summary(commandName).newTask();
    }

    public static ProcessTaskWrapper<String> retrieveHighstate() {
        return SshEffectorTasks.ssh(
            sudo("salt-call --local state.show_highstate --out=yaml"))
            .summary("retrieve highstate")
            .requiringZeroAndReturningStdout()
            .newTask();
    }
}
