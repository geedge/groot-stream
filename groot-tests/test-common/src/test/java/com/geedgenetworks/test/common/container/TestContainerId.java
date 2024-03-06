package com.geedgenetworks.test.common.container;

import lombok.AllArgsConstructor;
import lombok.Getter;

import static com.geedgenetworks.test.common.container.EngineType.FLINK;
@Getter
@AllArgsConstructor
public enum TestContainerId {
    FLINK_1_13(FLINK, "1.13.6");
    private final EngineType engineType;
    private final String version;

    @Override
    public String toString() {
        return engineType.toString() + ":" + version;
    }

}
