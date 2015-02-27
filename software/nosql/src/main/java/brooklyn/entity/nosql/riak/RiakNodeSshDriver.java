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
package brooklyn.entity.nosql.riak;

import static brooklyn.util.ssh.BashCommands.*;
import static java.lang.String.format;

import java.util.List;
import java.util.Map;

import brooklyn.util.ssh.BashCommands;
import com.google.api.client.util.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import brooklyn.entity.basic.AbstractSoftwareProcessSshDriver;
import brooklyn.entity.basic.Attributes;
import brooklyn.entity.basic.Entities;
import brooklyn.entity.basic.lifecycle.ScriptHelper;
import brooklyn.entity.software.SshEffectorTasks;
import brooklyn.location.OsDetails;
import brooklyn.location.basic.SshMachineLocation;
import brooklyn.util.collections.MutableMap;
import brooklyn.util.net.Urls;
import brooklyn.util.os.Os;
import brooklyn.util.task.DynamicTasks;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

// TODO: Alter -env ERL_CRASH_DUMP path in vm.args
public class RiakNodeSshDriver extends AbstractSoftwareProcessSshDriver implements RiakNodeDriver {

    private static final Logger LOG = LoggerFactory.getLogger(RiakNodeSshDriver.class);
    private static final String sbinPath = "$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";
    private boolean isPackageInstall = true;
    private boolean isRiakOnPath = true;

    public RiakNodeSshDriver(final RiakNodeImpl entity, final SshMachineLocation machine) {
        super(entity, machine);
    }

    @Override
    public RiakNodeImpl getEntity() {
        return RiakNodeImpl.class.cast(super.getEntity());
    }

    @Override
    public Map<String, String> getShellEnvironment() {
        MutableMap<String, String> result = MutableMap.copyOf(super.getShellEnvironment());
        // how to change epmd port, according to 
        // http://serverfault.com/questions/582787/how-to-change-listening-interface-of-rabbitmqs-epmd-port-4369
        if (getEntity().getEpmdListenerPort() != null) {
            result.put("ERL_EPMD_PORT", "" + Integer.toString(getEntity().getEpmdListenerPort()));
        }
        result.put("WAIT_FOR_ERLANG", "60");
        return result;
    }

    @Override
    public void preInstall() {
        resolver = Entities.newDownloader(this);
        setExpandedInstallDir(Os.mergePaths(getInstallDir(), resolver.getUnpackedDirectoryName(format("riak-%s", getVersion()))));
    }

    @Override
    public void install() {
        if (entity.getConfig(Attributes.DOWNLOAD_URL) != null) {
            LOG.warn("Ignoring download.url {}, use download.url.rhelcentos or download.url.mac", entity.getConfig(Attributes.DOWNLOAD_URL));
        }

        OsDetails osDetails = getMachine().getMachineDetails().getOsDetails();
        List<String> commands = Lists.newLinkedList();
        if (osDetails.isLinux()) {
            commands.addAll(installFromPackageCloud());
        } else if (osDetails.isMac()) {
            isPackageInstall = false;
            commands.addAll(installMac());
        } else if (osDetails.isWindows()) {
            throw new UnsupportedOperationException("RiakNode not supported on Windows instances");
        } else {
            throw new IllegalStateException("Machine was not detected as linux, mac or windows! Installation does not know how to proceed with " +
                    getMachine() + ". Details: " + getMachine().getMachineDetails().getOsDetails());
        }
        newScript(INSTALLING)
                .body.append(commands)
                .failIfBodyEmpty()
                .failOnNonZeroResultCode()
                .execute();
    }

    private List<String> installFromPackageCloud() {
        OsDetails osDetails = getMachine().getMachineDetails().getOsDetails();
        return ImmutableList.<String>builder()
                .add(osDetails.getName().toLowerCase().contains("debian") ? addSbinPathCommand() : "")
                .add(ifNotExecutable("curl", Joiner.on('\n').join(installCurl())))
                .addAll(ifExecutableElse("yum", installDebianBased(), installRpmBased()))
                .build();
    }

    public List<String> installCurl() {
        return ImmutableList.<String>builder()
                .add(ifExecutableElse("yum",
                        BashCommands.sudo("apt-get install --assume-yes curl"),
                        BashCommands.sudo("yum install -y curl")))
                .build();
    }

    private ImmutableList<String> installDebianBased() {
        return ImmutableList.<String>builder()
                .add("curl https://packagecloud.io/install/repositories/basho/riak/script.deb | " + BashCommands.sudo("bash"))
                .add(BashCommands.sudo("apt-get install --assume-yes riak"))
                .build();
    }

    private ImmutableList<String> installRpmBased() {
        return ImmutableList.<String>builder()
                .add("curl https://packagecloud.io/install/repositories/basho/riak/script.rpm | " + BashCommands.sudo("bash"))
                .add(BashCommands.sudo("yum install -y riak"))
                .build();
    }

    private static String addSbinPathCommand() {
        return "export PATH=$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";
    }

    /**
     * Returns a command which
     * executes <code>statement</code> only if <code>command</code> is NOT found in <code>$PATH</code>
     *
     * @param command
     * @param statement
     * @return command
     */
    private static String ifNotExecutable(String command, String statement) {
        return String.format("{ { test ! -z `which %s`; } || { %s; } }", command, statement);
    }

    private static String ifExecutableElse(String command, String ifTrue, String otherwise) {
        return com.google.common.base.Joiner.on('\n').join(
                ifExecutableElse(command, ImmutableList.<String>of(ifTrue), ImmutableList.<String>of(otherwise)));
    }

    private static ImmutableList<String> ifExecutableElse(String command, List<String> ifTrue, List<String> otherwise) {
        return ImmutableList.<String>builder()
                .add(String.format("if test -z `which %s`; then", command))
                .addAll(ifTrue)
                .add("else")
                .addAll(otherwise)
                .add("fi")
                .build();
    }

    protected List<String> installMac() {
        String saveAs = resolver.getFilename();
        String url = entity.getAttribute(RiakNode.DOWNLOAD_URL_MAC).toString();
        return ImmutableList.<String>builder()
                .add(INSTALL_TAR)
                .add(INSTALL_CURL)
                .add(commandToDownloadUrlAs(url, saveAs))
                .add("tar xzvf " + saveAs)
                .build();
    }

    @Override
    public void customize() {
        //create entity's runDir
        newScript(CUSTOMIZING).execute();

        isRiakOnPath = isPackageInstall ? isRiakOnPath() : true;

        OsDetails osDetails = getMachine().getMachineDetails().getOsDetails();

        List<String> commands = Lists.newLinkedList();
        commands.add(sudo("mkdir -p " + getRiakEtcDir()));

        if (isVersion1()) {
            String vmArgsTemplate = processTemplate(entity.getConfig(RiakNode.RIAK_VM_ARGS_TEMPLATE_URL));
            String saveAsVmArgs = Urls.mergePaths(getRunDir(), "vm.args");
            DynamicTasks.queueIfPossible(SshEffectorTasks.put(saveAsVmArgs).contents(vmArgsTemplate));
            commands.add(sudo("mv " + saveAsVmArgs + " " + getRiakEtcDir()));

            String appConfigTemplate = processTemplate(entity.getConfig(RiakNode.RIAK_APP_CONFIG_TEMPLATE_URL));
            String saveAsAppConfig = Urls.mergePaths(getRunDir(), "app.config");
            DynamicTasks.queueIfPossible(SshEffectorTasks.put(saveAsAppConfig).contents(appConfigTemplate));
            commands.add(sudo("mv " + saveAsAppConfig + " " + getRiakEtcDir()));
        } else {
            String templateUrl = osDetails.isMac() ? entity.getConfig(RiakNode.RIAK_CONF_TEMPLATE_URL_MAC) :
                    entity.getConfig(RiakNode.RIAK_CONF_TEMPLATE_URL_LINUX);
            String riakConfTemplate = processTemplate(templateUrl);
            String saveAsRiakConf = Urls.mergePaths(getRunDir(), "riak.conf");
            DynamicTasks.queueIfPossible(SshEffectorTasks.put(saveAsRiakConf).contents(riakConfTemplate));
            commands.add(sudo("mv " + saveAsRiakConf + " " + getRiakEtcDir()));
        }

        //increase open file limit (default min for riak is: 4096)
        //TODO: detect the actual limit then do the modification.
        //TODO: modify ulimit for linux distros
        //    commands.add(sudo("launchctl limit maxfiles 4096 32768"));
        if (osDetails.isMac()) {
            commands.add("ulimit -n 4096");
        } else if (osDetails.isLinux() && isVersion1()) {
            commands.add(sudo("chown -R riak:riak " + getRiakEtcDir()));
        }

        ScriptHelper customizeScript = newScript(CUSTOMIZING)
                .failOnNonZeroResultCode()
                .body.append(commands);

        if (!isRiakOnPath) {
            Map<String, String> newPathVariable = ImmutableMap.of("PATH", sbinPath);
            log.warn("riak command not found on PATH. Altering future commands' environment variables from {} to {}", getShellEnvironment(), newPathVariable);
            customizeScript.environmentVariablesReset(newPathVariable);
        }
        customizeScript.failOnNonZeroResultCode().execute();

        //set the riak node name
        entity.setAttribute(RiakNode.RIAK_NODE_NAME, format("riak@%s", getSubnetHostname()));
    }

    @Override
    public void launch() {
        List<String> commands = Lists.newLinkedList();
        if (isPackageInstall) {
            commands.add(addSbinPathCommand());
            commands.add(sudo("service riak start"));
        } else {
            // NOTE: See instructions at http://superuser.com/questions/433746/is-there-a-fix-for-the-too-many-open-files-in-system-error-on-os-x-10-7-1
            // for increasing the system limit for number of open files
            commands.add("ulimit -n 65536 || true"); // `BashCommands.ok` will put this in parentheses, which will set ulimit -n in the subshell
            commands.add(format("%s start >/dev/null 2>&1 < /dev/null &", getRiakCmd()));
        }

        ScriptHelper launchScript = newScript(LAUNCHING)
                .body.append(commands);

        if (!isRiakOnPath) {
            Map<String, String> newPathVariable = ImmutableMap.of("PATH", sbinPath);
            log.warn("riak command not found on PATH. Altering future commands' environment variables from {} to {}", getShellEnvironment(), newPathVariable);
            launchScript.environmentVariablesReset(newPathVariable);
        }
        launchScript.failOnNonZeroResultCode().execute();
    }

    @Override
    public void stop() {
        leaveCluster();

        String command = format("%s stop", getRiakCmd());
        command = isPackageInstall ? sudo(command) : command;

        ScriptHelper stopScript = newScript(ImmutableMap.of(USE_PID_FILE, false), STOPPING)
                .body.append(command);

        if (!isRiakOnPath) {
            Map<String, String> newPathVariable = ImmutableMap.of("PATH", sbinPath);
            log.warn("riak command not found on PATH. Altering future commands' environment variables from {} to {}", getShellEnvironment(), newPathVariable);
            stopScript.environmentVariablesReset(newPathVariable);
        }

        int result = stopScript.failOnNonZeroResultCode().execute();
        if (result != 0) {
            newScript(ImmutableMap.of(USE_PID_FILE, false), STOPPING).execute();
        }
    }

    @Override
    public boolean isRunning() {
        // Version 2.0.0 requires sudo for `riak ping`
        ScriptHelper checkRunningScript = newScript(CHECK_RUNNING)
                .body.append(sudo(format("%s ping", getRiakCmd())));

        if (!isRiakOnPath) {
            Map<String, String> newPathVariable = ImmutableMap.of("PATH", sbinPath);
            log.warn("riak command not found on PATH. Altering future commands' environment variables from {} to {}", getShellEnvironment(), newPathVariable);
            checkRunningScript.environmentVariablesReset(newPathVariable);
        }
        return (checkRunningScript.execute() == 0);
    }

    public String getRiakEtcDir() {
        return isPackageInstall ? "/etc/riak" : Urls.mergePaths(getExpandedInstallDir(), "etc");
    }

    protected String getRiakCmd() {
        return isPackageInstall ? "riak" : Urls.mergePaths(getExpandedInstallDir(), "bin/riak");
    }

    protected String getRiakAdminCmd() {
        return isPackageInstall ? "riak-admin" : Urls.mergePaths(getExpandedInstallDir(), "bin/riak-admin");
    }

    @Override
    public void joinCluster(String nodeName) {
        //FIXME: find a way to batch commit the changes, instead of committing for every operation.

        if (getRiakName().equals(nodeName)) {
            log.warn("cannot join riak node: {} to itself", nodeName);
        } else {
            if (!hasJoinedCluster()) {

                ScriptHelper joinClusterScript = newScript("joinCluster")
                        .body.append(sudo(format("%s cluster join %s", getRiakAdminCmd(), nodeName)))
                        .failOnNonZeroResultCode();

                if (!isRiakOnPath) {
                    Map<String, String> newPathVariable = ImmutableMap.of("PATH", sbinPath);
                    log.warn("riak command not found on PATH. Altering future commands' environment variables from {} to {}", getShellEnvironment(), newPathVariable);
                    joinClusterScript.environmentVariablesReset(newPathVariable);
                }

                joinClusterScript.execute();

                entity.setAttribute(RiakNode.RIAK_NODE_HAS_JOINED_CLUSTER, Boolean.TRUE);
            } else {
                log.warn("entity {}: is already in the riak cluster", entity.getId());
            }
        }
    }

    @Override
    public void leaveCluster() {
        //TODO: add 'riak-admin cluster force-remove' for erroneous and unrecoverable nodes.
        //FIXME: find a way to batch commit the changes, instead of committing for every operation.
        //FIXME: find a way to check if the node is the last in the cluster to avoid removing the only member and getting "last node error"

        if (hasJoinedCluster()) {
            ScriptHelper leaveClusterScript = newScript("leaveCluster")
                    .body.append(sudo(format("%s cluster leave", getRiakAdminCmd())))
                    .body.append(sudo(format("%s cluster plan", getRiakAdminCmd())))
                    .body.append(sudo(format("%s cluster commit", getRiakAdminCmd())))
                    .failOnNonZeroResultCode();

            if (!isRiakOnPath) {
                Map<String, String> newPathVariable = ImmutableMap.of("PATH", sbinPath);
                log.warn("riak command not found on PATH. Altering future commands' environment variables from {} to {}", getShellEnvironment(), newPathVariable);
                leaveClusterScript.environmentVariablesReset(newPathVariable);
            }

            leaveClusterScript.execute();

            entity.setAttribute(RiakNode.RIAK_NODE_HAS_JOINED_CLUSTER, Boolean.FALSE);
        } else {
            log.warn("entity {}: is not in the riak cluster", entity.getId());
        }
    }

    @Override
    public void commitCluster() {
        if (hasJoinedCluster()) {
            ScriptHelper commitClusterScript = newScript("commitCluster")
                    .body.append(sudo(format("%s cluster plan", getRiakAdminCmd())))
                    .body.append(sudo(format("%s cluster commit", getRiakAdminCmd())))
                    .failOnNonZeroResultCode();

            if (!isRiakOnPath) {
                Map<String, String> newPathVariable = ImmutableMap.of("PATH", sbinPath);
                log.warn("riak command not found on PATH. Altering future commands' environment variables from {} to {}", getShellEnvironment(), newPathVariable);
                commitClusterScript.environmentVariablesReset(newPathVariable);
            }
            commitClusterScript.execute();

        } else {
            log.warn("entity {}: is not in the riak cluster", entity.getId());
        }
    }

    @Override
    public void recoverFailedNode(String nodeName) {

        //TODO find ways to detect a faulty/failed node
        //argument passed 'node' is any working node in the riak cluster
        //following the instruction from: http://docs.basho.com/riak/latest/ops/running/recovery/failed-node/

        if (hasJoinedCluster()) {
            String failedNodeName = getRiakName();


            String stopCommand = format("%s stop", getRiakCmd());
            stopCommand = isPackageInstall ? sudo(stopCommand) : stopCommand;

            String startCommand = format("%s start >/dev/null 2>&1 < /dev/null &", getRiakCmd());
            startCommand = isPackageInstall ? sudo(startCommand) : startCommand;

            ScriptHelper recoverNodeScript = newScript("recoverNode")
                    .body.append(stopCommand)
                    .body.append(format("%s down %s", getRiakAdminCmd(), failedNodeName))
                    .body.append(sudo(format("rm -rf %s", getRingStateDir())))
                    .body.append(startCommand)
                    .body.append(sudo(format("%s cluster join %s", getRiakAdminCmd(), nodeName)))
                    .body.append(sudo(format("%s cluster plan", getRiakAdminCmd())))
                    .body.append(sudo(format("%s cluster commit", getRiakAdminCmd())))
                    .failOnNonZeroResultCode();

            if (!isRiakOnPath) {
                Map<String, String> newPathVariable = ImmutableMap.of("PATH", sbinPath);
                log.warn("riak command not found on PATH. Altering future commands' environment variables from {} to {}", getShellEnvironment(), newPathVariable);
                recoverNodeScript.environmentVariablesReset(newPathVariable);
            }

            recoverNodeScript.execute();

        } else {
            log.warn("entity {}: is not in the riak cluster", entity.getId());
        }
    }

    private Boolean hasJoinedCluster() {
        return ((RiakNode) entity).hasJoinedCluster();
    }

    protected boolean isRiakOnPath() {
        return (newScript("riakOnPath")
                .body.append("which riak")
                .execute() == 0);
    }

    private String getRiakName() {
        return entity.getAttribute(RiakNode.RIAK_NODE_NAME);
    }

    private String getRingStateDir() {
        //TODO: check for non-package install.
        return isPackageInstall ? "/var/lib/riak/ring" : Urls.mergePaths(getExpandedInstallDir(), "lib/ring");
    }

    protected boolean isVersion1() {
        return getVersion().startsWith("1.");
    }

    @Override
    public String getOsMajorVersion() {
        OsDetails osDetails = getMachine().getMachineDetails().getOsDetails();
        String osVersion = osDetails.getVersion();
        return osVersion.contains(".") ? osVersion.substring(0, osVersion.indexOf(".")) : osVersion;
    }
}
