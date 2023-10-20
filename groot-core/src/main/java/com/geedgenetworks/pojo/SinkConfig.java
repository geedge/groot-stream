package com.geedgenetworks.pojo;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class SinkConfig implements Serializable {

    private int parallelism;
    private Map<String, Object> properties;
    private String format;
    private String type;
    private List<String> output_fields;
    private String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<String> getOutput_fields() {
        return output_fields;
    }

    public void setOutput_fields(List<String> output_fields) {
        this.output_fields = output_fields;
    }
}
