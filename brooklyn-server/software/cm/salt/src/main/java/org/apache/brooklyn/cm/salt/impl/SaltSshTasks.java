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

import org.apache.brooklyn.api.mgmt.TaskAdaptable;
import org.apache.brooklyn.api.mgmt.TaskFactory;
import org.apache.brooklyn.core.effector.EffectorTasks;
import org.apache.brooklyn.core.effector.ssh.SshEffectorTasks;
import org.apache.brooklyn.core.effector.ssh.SshEffectorTasks.SshEffectorTaskFactory;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.core.file.ArchiveTasks;
import org.apache.brooklyn.util.core.task.TaskBuilder;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.core.task.ssh.SshPutTaskFactory;
import org.apache.brooklyn.util.core.task.system.ProcessTaskWrapper;
import org.apache.brooklyn.util.ssh.BashCommands;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.text.Strings;

import java.util.List;
import java.util.Set;

import static org.apache.brooklyn.core.effector.ssh.SshEffectorTasks.ssh;
import static org.apache.brooklyn.util.ssh.BashCommands.sudo;


public class SaltSshTasks {

    private static final String UTILITY_SCRIPT = "salt_utilities.sh";

    private SaltSshTasks() {
        // Utility class
    }

    public static TaskFactory<?> installSalt(boolean force) {
        // TODO: ignore force?
        List<String> commands = MutableList.<String>builder()
            .add(BashCommands.commandToDownloadUrlAs("https://bootstrap.saltstack.com", "install_salt.sh"))
            .add(sudo("sh install_salt.sh"))
            .build();
        return ssh(commands).summary("install salt");
    }

    public static TaskFactory<?> configureForMasterlessOperation(boolean force) {
        // TODO: ignore force?
        return ssh(sudo("sed -i '/^#file_client/c file_client: local' /etc/salt/minion"))
            .summary("configure masterless");
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
        return ssh(Strings.join(commandLines, "\n"))
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

                // TODO move this into salt_utilities.yaml
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
                tb.add(ssh(installCmd).summary("installing " + formulaUrl + " states to /srv/formula")
                    .requiringExitCodeZero().newTask());

                return tb.build();
            }
        };
    }

    public static TaskFactory<?> installTopFile(final Set<? extends String> runList, boolean force) {
        // TODO: ignore force?
        // TODO: move this into salt_utilities.sh
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
        return ssh(commands).summary("create top.sls file");

    }

    public static TaskAdaptable applyTopStates(boolean force) {
        return ssh(sudo("salt-call --local state.apply")).summary("apply top states").newTask();
    }

    public static TaskAdaptable applyState(String state, boolean force) {
        final String commandName = "state.apply " + state;
        return ssh(sudo("salt-call --local " + commandName)).summary(commandName).newTask();
    }

    public static ProcessTaskWrapper<String> retrieveHighstate() {
        return ssh(sudo("salt-call --local state.show_highstate --out=yaml"))
            .summary("retrieve highstate")
            .requiringZeroAndReturningStdout()
            .newTask();
    }


    public static TaskFactory<?> installSaltUtilities(boolean force) {
        return new TaskFactory<TaskAdaptable<?>>() {
            @Override
            public TaskAdaptable<?> newTask() {
                final TaskBuilder<Void> builder = Tasks.<Void>builder()
                    .displayName("install salt utilities")
                    .add(installScript(UTILITY_SCRIPT, "install salt shell utils").newTask())
                    .add(ssh(sudo("mv /tmp/" + UTILITY_SCRIPT + " /etc/salt")).newTask());
                return builder.build();
            }
        };
    }

    private static SshPutTaskFactory installScript(String name, String description) {
        return SshEffectorTasks.put("/tmp/" + name)
                .contents(ResourceUtils.create().getResourceFromUrl("classpath:" + name))
                .summary(description);
    }

    public static SshEffectorTaskFactory<Integer> verifyStates(Set<String> states, String description, boolean force) {
        return invokeStatesUtility("verify_states", states, description);
    }

    public static SshEffectorTaskFactory<Integer> findStates(Set<String> states, String description, boolean force) {
        return invokeStatesUtility("find_states", states, description);
    }

    private static SshEffectorTaskFactory<Integer> invokeStatesUtility(String functionName, Set<String> states,
            String description) {

        return ssh(sudo("/bin/bash -c '. /etc/salt/salt_utilities.sh ; " + functionName + " "
            + Strings.join(states, " ") + "'"))
            .allowingNonZeroExitCode()
            .summary(description);
    }

}
