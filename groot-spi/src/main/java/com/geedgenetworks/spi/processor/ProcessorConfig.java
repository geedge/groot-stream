package com.geedgenetworks.spi.processor;

import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

@Data
public class ProcessorConfig implements Serializable {
    private String type;
    private int parallelism;
    private String name;
    private List<String> output_fields;
    private List<String> remove_fields;
    private Map<String, Object> properties;
}
