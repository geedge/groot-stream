package com.geedgenetworks.api.processor;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
public class ProcessorConfig implements Serializable {
    private String name;
    private String type;
    private int parallelism;
    private Map<String, Object> properties;
}
