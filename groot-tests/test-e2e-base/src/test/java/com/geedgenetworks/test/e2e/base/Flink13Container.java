package com.geedgenetworks.test.e2e.base;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.Container;

import java.io.IOException;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class Flink13Container extends com.geedgenetworks.test.common.container.Flink13Container {
    @Override
    @BeforeAll
    public void startUp() throws Exception {
        super.startUp();
        log.info("The TestContainer[{}] is running.", identifier());
    }

    @Override
    @AfterAll
    public void tearDown() throws Exception {
        super.tearDown();
        log.info("The TestContainer[{}] is closed.", identifier());
    }

    public Container.ExecResult executeGrootStreamJob(String confFile)
            throws IOException, InterruptedException {
        return executeJob(confFile);
    }


}
