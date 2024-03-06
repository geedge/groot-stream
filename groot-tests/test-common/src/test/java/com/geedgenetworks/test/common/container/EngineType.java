package com.geedgenetworks.test.common.container;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum EngineType {
    FLINK("Flink");
    private final String name;
    @Override
    public String toString() {
        return name;
    }

}
