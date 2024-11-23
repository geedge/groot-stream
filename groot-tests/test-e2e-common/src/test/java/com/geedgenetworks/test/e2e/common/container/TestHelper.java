package com.geedgenetworks.test.e2e.common.container;

import org.junit.jupiter.api.Assertions;
import org.testcontainers.containers.Container;

import java.io.IOException;

public class TestHelper {
    private final TestContainer container;

    public TestHelper(TestContainer container) {
        this.container = container;
    }

    public void execute(String file) throws IOException, InterruptedException {
        execute(0, file);
    }

    public void execute(int exceptResult, String file) throws IOException, InterruptedException {
        Container.ExecResult result = container.executeJob(file);
        Assertions.assertEquals(exceptResult, result.getExitCode(), result.getStderr());
    }
}
