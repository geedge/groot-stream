package com.geedgenetworks.test.e2e.common.container;

import com.google.common.collect.Lists;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

@NoArgsConstructor
@Slf4j
public abstract class AbstractTestFlinkContainer extends AbstractTestContainer {
    protected static final List<String> DEFAULT_FLINK_PROPERTIES =
            Arrays.asList(
                    "jobmanager.rpc.address: jobmanager",
                    "taskmanager.numberOfTaskSlots: 10",
                    "parallelism.default: 3",
                    "env.java.opts: -Doracle.jdbc.timezoneAsRegion=false");

    protected static final String DEFAULT_DOCKER_IMAGE = "flink:1.13.1-scala_2.11-java11";

    protected GenericContainer<?> jobManager;
    protected GenericContainer<?> taskManager;

    @Override
    protected String getDockerImage() {
        return DEFAULT_DOCKER_IMAGE;
    }

    @Override
    public void startUp() throws Exception {
        final String dockerImage = getDockerImage();
        final String properties = String.join("\n", getFlinkProperties());
        jobManager =
                new GenericContainer<>(dockerImage)
                        .withCommand("jobmanager")
                        .withNetwork(NETWORK)
                        .withNetworkAliases("jobmanager")
                        .withExposedPorts()
                        .withEnv("FLINK_PROPERTIES", properties)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(dockerImage + ":jobmanager")))
                        .waitingFor(
                                new LogMessageWaitStrategy()
                                        .withRegEx(".*Starting the resource manager.*")
                                        .withStartupTimeout(Duration.ofMinutes(2)))
                        ;

        // Copy groot-stream bootstrap and some other files to the container
        copyGrootStreamStarterToContainer(jobManager);
        copyGrootStreamStarterLoggingToContainer(jobManager);

        jobManager.setPortBindings(Lists.newArrayList(String.format("%s:%s", 8999, 8081)));

        taskManager =
                new GenericContainer<>(dockerImage)
                        .withCommand("taskmanager")
                        .withNetwork(NETWORK)
                        .withNetworkAliases("taskmanager")
                        .withEnv("FLINK_PROPERTIES", properties)
                        .dependsOn(jobManager)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(
                                                dockerImage + ":taskmanager")))
                        .waitingFor(
                                new LogMessageWaitStrategy()
                                        .withRegEx(
                                                ".*Successful registration at resource manager.*")
                                        .withStartupTimeout(Duration.ofMinutes(2)));

        // Copy groot-stream bootstrap and some other files to the container
        copyGrootStreamStarterToContainer(taskManager);
        copyGrootStreamStarterLoggingToContainer(taskManager);

        Startables.deepStart(Stream.of(jobManager)).join();
        Startables.deepStart(Stream.of(taskManager)).join();
        // execute extra commands
        executeExtraCommands(jobManager);
    }

    protected List<String> getFlinkProperties() {
        return DEFAULT_FLINK_PROPERTIES;
    }

    @Override
    public void tearDown() throws Exception {
        if (taskManager != null) {
            taskManager.stop();
        }
        if (jobManager != null) {
            jobManager.stop();
        }
    }

    @Override
    protected String getSavePointCommand() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected String getRestoreCommand() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    protected List<String> getExtraStartShellCommands() {
        return Collections.emptyList();
    }


    public void executeExtraCommands(ContainerExtendedFactory extendedFactory)
            throws IOException, InterruptedException {
        extendedFactory.extend(jobManager);
        extendedFactory.extend(taskManager);
    }


    @Override
    public Container.ExecResult executeJob(String confFile)
            throws IOException, InterruptedException {
        return executeJob(confFile, null);
    }

    @Override
    public Container.ExecResult executeJob(String confFile, List<String> variables)
            throws IOException, InterruptedException {
        log.info("test in container: {}", identifier());
        return executeJob(jobManager, confFile, variables);
    }

    @Override
    public String getServerLogs() {
        return jobManager.getLogs() + "\n" + taskManager.getLogs();
    }


    public String executeJobManagerInnerCommand(String command)
            throws IOException, InterruptedException {
        return jobManager.execInContainer("bash", "-c", command).getStdout();
    }




}
