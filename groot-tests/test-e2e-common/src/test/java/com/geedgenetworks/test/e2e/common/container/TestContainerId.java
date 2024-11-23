package com.geedgenetworks.test.e2e.common.container;

import lombok.AllArgsConstructor;
import lombok.Getter;

import static com.geedgenetworks.test.e2e.common.container.EngineType.FLINK;
@Getter
@AllArgsConstructor
public enum TestContainerId {
    FLINK_1_13(FLINK, "1.13.1"),
    FLINK_1_17(FLINK, "1.17.2");
    private final EngineType engineType;
    private final String version;

    @Override
    public String toString() {
        return engineType.toString() + ":" + version;
    }

}
