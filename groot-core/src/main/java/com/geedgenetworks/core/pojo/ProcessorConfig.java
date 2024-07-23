package com.geedgenetworks.core.pojo;

import lombok.Data;

import java.util.Map;
@Data
public class ProcessorConfig {
    private String type;
    private int parallelism;
    private Map<String, Object> properties;
    private String name;
}
