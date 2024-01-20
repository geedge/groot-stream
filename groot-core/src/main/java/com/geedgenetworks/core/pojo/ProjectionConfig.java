package com.geedgenetworks.core.pojo;

import com.geedgenetworks.common.udf.UDFContext;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class ProjectionConfig implements Serializable {

    private int parallelism;
    private List<UDFContext> functions;
    private String format;
    private String type;
    private List<String> output_fields;
    private List<String> remove_fields;
    private Map<String, Object> properties;
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

    public List<UDFContext> getFunctions() {
        return functions;
    }

    public void setFunctions(List<UDFContext> functions) {
        this.functions = functions;
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

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }

    public List<String> getRemove_fields() {
        return remove_fields;
    }

    public void setRemove_fields(List<String> remove_fields) {
        this.remove_fields = remove_fields;
    }
}
