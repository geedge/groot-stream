package com.geedgenetworks.pojo;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class AggregateFunction implements Serializable {

    private String name;
    private List<String> input_fields;
    private List<String> output_fields;
    private String filter;
    private Map<String, Object> parameters;
    private String class_name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getInput_fields() {
        return input_fields;
    }

    public void setInput_fields(List<String> input_fields) {
        this.input_fields = input_fields;
    }

    public List<String> getOutput_fields() {
        return output_fields;
    }

    public void setOutput_fields(List<String> output_fields) {
        this.output_fields = output_fields;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    public void setParameters(Map<String, Object> parameters) {
        this.parameters = parameters;
    }

    public String getClass_name() {
        return class_name;
    }

    public void setClass_name(String class_name) {
        this.class_name = class_name;
    }
}
