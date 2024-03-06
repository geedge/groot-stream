package com.geedgenetworks.test.e2e.base;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;

import java.io.IOException;

public class InlineToPrint extends Flink13Container{

    @Test
    public void testInlineToPrint() throws IOException, InterruptedException {
        Container.ExecResult execResult = executeGrootStreamJob("/inline_to_print.yaml");
        Assertions.assertEquals(0, execResult.getExitCode());
    }
}
