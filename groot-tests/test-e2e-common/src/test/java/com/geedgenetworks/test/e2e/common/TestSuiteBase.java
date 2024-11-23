package com.geedgenetworks.test.e2e.common;

import com.geedgenetworks.test.e2e.common.container.ContainerUtil;
import com.geedgenetworks.test.e2e.common.container.TestContainer;
import com.geedgenetworks.test.e2e.common.container.TestContainersFactory;
import com.geedgenetworks.test.e2e.common.junit.ContainerTestingExtension;
import com.geedgenetworks.test.e2e.common.junit.TestCaseInvocationContextProvider;
import com.geedgenetworks.test.e2e.common.junit.TestContainers;
import com.geedgenetworks.test.e2e.common.junit.TestLoggerExtension;
import com.github.dockerjava.api.DockerClient;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Network;

@ExtendWith({
        ContainerTestingExtension.class,
        TestLoggerExtension.class,
        TestCaseInvocationContextProvider.class
})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestSuiteBase {
    protected static final Network NETWORK = TestContainer.NETWORK;
    @TestContainers
    private TestContainersFactory containersFactory = ContainerUtil::discoverTestContainers;
    protected DockerClient dockerClient = DockerClientFactory.lazyClient();


}
