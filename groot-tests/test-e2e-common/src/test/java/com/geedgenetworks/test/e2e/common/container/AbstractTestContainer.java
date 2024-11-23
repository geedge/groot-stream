package com.geedgenetworks.test.e2e.common.container;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import static com.geedgenetworks.test.e2e.common.container.ContainerUtil.PROJECT_ROOT_PATH;
public abstract class AbstractTestContainer implements TestContainer {
    protected static final Logger LOG = LoggerFactory.getLogger(AbstractTestContainer.class);

    public static final String GROOTSTREAM_HOME = "/tmp/grootstream/";

    protected final String startModuleName;

    protected final String startModuleFullPath;

    public AbstractTestContainer() {
        this.startModuleName = getStartModuleName();
        this.startModuleFullPath =
                PROJECT_ROOT_PATH
                        + File.separator
                        + this.startModuleName;
        ContainerUtil.checkPathExist(startModuleFullPath);
    }

    protected abstract String getDockerImage();

    protected abstract String getStartModuleName();

    protected abstract String getStartShellName();

    protected abstract String getConnectorModulePath();

    protected abstract String getConnectorType();

    protected abstract String getSavePointCommand();

    protected abstract String getRestoreCommand();

    protected abstract String getConnectorNamePrefix();

    protected abstract List<String> getExtraStartShellCommands();

    protected void executeExtraCommands(GenericContainer<?> container)
            throws IOException, InterruptedException {
        // do nothing
    }

    protected void copyGrootStreamStarterToContainer(GenericContainer<?> container) {
        ContainerUtil.copyGrootStreamStarterToContainer(
                container, this.startModuleName, this.startModuleFullPath, GROOTSTREAM_HOME);
    }

    protected void copyGrootStreamStarterLoggingToContainer(GenericContainer<?> container) {
        ContainerUtil.copyGrootStreamStarterLoggingToContainer(
                container, this.startModuleFullPath, GROOTSTREAM_HOME);
    }

    protected Container.ExecResult executeJob(GenericContainer<?> container, String confFile, List<String> variables)
            throws IOException, InterruptedException {

        final String confInContainerPath =  ContainerUtil.copyConfigFileToContainer(container, confFile);
        // copy connectors
        ContainerUtil.copyConnectorJarToContainer(
                container,
                confFile,
                getConnectorModulePath(),
                getConnectorNamePrefix(),
                getConnectorType(),
                GROOTSTREAM_HOME);
        final List<String> command = new ArrayList<>();
        String binPath = Paths.get(GROOTSTREAM_HOME, "bin", getStartShellName()).toString();
        // base command
        command.add(ContainerUtil.adaptPathForWin(binPath));
        command.add("--config");
        command.add(ContainerUtil.adaptPathForWin(confInContainerPath));
        command.add("--target");
        command.add("remote");
        List<String> extraStartShellCommands = new ArrayList<>(getExtraStartShellCommands());
        if (variables != null && !variables.isEmpty()) {
            variables.forEach(
                    v -> {
                        extraStartShellCommands.add("-i");
                        extraStartShellCommands.add(v);
                    });
        }
        command.addAll(extraStartShellCommands);
        return executeCommand(container, command);
    }



    protected Container.ExecResult executeJob(GenericContainer<?> container, String confFile)
            throws IOException, InterruptedException {
        return executeJob(container, confFile, null);
    }



    protected Container.ExecResult savepointJob(GenericContainer<?> container, String jobId)
            throws IOException, InterruptedException {
        final List<String> command = new ArrayList<>();
        String binPath = Paths.get(GROOTSTREAM_HOME, "bin", getStartShellName()).toString();
        // base command
        command.add(ContainerUtil.adaptPathForWin(binPath));
        command.add(getSavePointCommand());
        command.add(jobId);
        command.addAll(getExtraStartShellCommands());
        return executeCommand(container, command);
    }

    protected Container.ExecResult restoreJob(
            GenericContainer<?> container, String confFile, String jobId)
            throws IOException, InterruptedException {
        final String confInContainerPath = ContainerUtil.copyConfigFileToContainer(container, confFile);
        // copy connectors
        ContainerUtil.copyConnectorJarToContainer(
                container,
                confFile,
                getConnectorModulePath(),
                getConnectorNamePrefix(),
                getConnectorType(),
                GROOTSTREAM_HOME);
        final List<String> command = new ArrayList<>();
        String binPath = Paths.get(GROOTSTREAM_HOME, "bin", getStartShellName()).toString();
        // base command
        command.add(ContainerUtil.adaptPathForWin(binPath));
        command.add("--config");
        command.add(ContainerUtil.adaptPathForWin(confInContainerPath));
        command.add(getRestoreCommand());
        command.add(jobId);
        command.addAll(getExtraStartShellCommands());
        return executeCommand(container, command);
    }

    protected Container.ExecResult executeCommand(
            GenericContainer<?> container, List<String> command)
            throws IOException, InterruptedException {
        String commandStr = String.join(" ", command);
        LOG.info(
                "Execute command in container[{}] "
                        + "\n==================== Shell Command start ====================\n"
                        + "{}"
                        + "\n==================== Shell Command end   ====================",
                container.getDockerImageName(),
                commandStr);
        Container.ExecResult execResult = container.execInContainer("bash", "-c", commandStr);

        if (execResult.getStdout() != null && !execResult.getStdout().isEmpty()) {
            LOG.info(
                    "Container[{}] command {} STDOUT:"
                            + "\n==================== STDOUT start ====================\n"
                            + "{}"
                            + "\n==================== STDOUT end   ====================",
                    container.getDockerImageName(),
                    commandStr,
                    execResult.getStdout());
        }
        if (execResult.getStderr() != null && !execResult.getStderr().isEmpty()) {
            LOG.error(
                    "Container[{}] command {} STDERR:"
                            + "\n==================== STDERR start ====================\n"
                            + "{}"
                            + "\n==================== STDERR end   ====================",
                    container.getDockerImageName(),
                    commandStr,
                    execResult.getStderr());
        }

        if (execResult.getExitCode() != 0) {
            LOG.info(
                    "Container[{}] command {} Server Log:"
                            + "\n==================== Server Log start ====================\n"
                            + "{}"
                            + "\n==================== Server Log end   ====================",
                    container.getDockerImageName(),
                    commandStr,
                    container.getLogs());
        }

        return execResult;
    }



}
