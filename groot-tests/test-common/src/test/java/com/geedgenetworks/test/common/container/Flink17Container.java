package com.geedgenetworks.test.common.container;

import com.google.auto.service.AutoService;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AutoService(TestContainer.class)
public class Flink17Container extends AbstractTestFlinkContainer {

    @Override
    protected String getStartModuleName() {
        return "groot-bootstrap";
    }

    @Override
    protected String getStartShellName() {
        return "start.sh";
    }

    @Override
    protected String getConnectorModulePath() {
        return "groot-connectors";
    }

    @Override
    protected String getConnectorType() {
        return "grootstream";
    }

    @Override
    protected String getConnectorNamePrefix() {
        return "connector-";
    }

    @Override
    public TestContainerId identifier() {
        return TestContainerId.FLINK_1_17 ;
    }
    @Override
    protected String getDockerImage() {
        return "flink:1.17.2-scala_2.12-java11";
    }
}
