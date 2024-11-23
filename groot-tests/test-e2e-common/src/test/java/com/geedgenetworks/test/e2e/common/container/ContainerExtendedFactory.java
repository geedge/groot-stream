package com.geedgenetworks.test.e2e.common.container;

import org.testcontainers.containers.GenericContainer;

import java.io.IOException;

@FunctionalInterface
public interface ContainerExtendedFactory {
    void extend(GenericContainer<?> engineMasterContainer) throws IOException, InterruptedException;

}
