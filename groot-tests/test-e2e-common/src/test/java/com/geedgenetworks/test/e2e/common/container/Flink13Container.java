package com.geedgenetworks.test.e2e.common.container;

import com.google.auto.service.AutoService;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AutoService(TestContainer.class)
public class Flink13Container extends AbstractTestFlinkContainer {

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
        return TestContainerId.FLINK_1_13;
    }

    @Override
    protected String getDockerImage() {
        return "192.168.40.153:8082/common/flink:1.13.1-scala_2.11-java11";
    }


}
