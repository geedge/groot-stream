package com.geedgenetworks.test.common.container;

import org.testcontainers.containers.GenericContainer;

import java.io.IOException;

@FunctionalInterface
public interface ContainerExtendedFactory {
    void extend(GenericContainer<?> engineMasterContainer) throws IOException, InterruptedException;

}
