package com.geedgenetworks.test.common.container;

import lombok.AllArgsConstructor;
import lombok.Getter;

import static com.geedgenetworks.test.common.container.EngineType.FLINK;
@Getter
@AllArgsConstructor
public enum TestContainerId {
    FLINK_1_13(FLINK, "1.13.1"),
    FLINK_1_14(FLINK, "1.14.6"),
    FLINK_1_15(FLINK, "1.15.3"),
    FLINK_1_16(FLINK, "1.16.0");
    private final EngineType engineType;
    private final String version;

    @Override
    public String toString() {
        return engineType.toString() + ":" + version;
    }

}
