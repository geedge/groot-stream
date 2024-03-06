package com.geedgenetworks.test.common.container;

import com.geedgenetworks.test.common.TestResource;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.Network;

import java.io.IOException;

public interface TestContainer extends TestResource {
    Network NETWORK = Network.newNetwork();
    TestContainerId identifier();

    void executeExtraCommands(ContainerExtendedFactory extendedFactory)
            throws IOException, InterruptedException;

    Container.ExecResult executeJob(String confFile) throws IOException, InterruptedException;


    default Container.ExecResult savepointJob(String jobId)
            throws IOException, InterruptedException {
        throw new UnsupportedOperationException("Not implemented");
    }

    default Container.ExecResult restoreJob(String confFile, String jobId)
            throws IOException, InterruptedException {
        throw new UnsupportedOperationException("Not implemented");
    }

    String getServerLogs();



}
