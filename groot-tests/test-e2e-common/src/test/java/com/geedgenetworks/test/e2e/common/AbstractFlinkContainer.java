package com.geedgenetworks.test.e2e.common;


import com.geedgenetworks.test.e2e.common.container.AbstractTestFlinkContainer;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.Container;

import java.io.IOException;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractFlinkContainer extends AbstractTestFlinkContainer {
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


    public Container.ExecResult executeGrootStreamFlinkJob(String confFile)
            throws IOException, InterruptedException {
        return executeJob(confFile);
    }


}
