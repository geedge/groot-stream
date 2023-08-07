package com.geedgenetworks.pojo;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class ProjectionFunction implements Serializable {

    private String name;
    private List<String> lookup_fields;
    private List<String> output_fields;
    private String filter;
    private Map<String,Object> parameters;
    private String class_name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getLookup_fields() {
        return lookup_fields;
    }

    public void setLookup_fields(List<String> lookup_fields) {
        this.lookup_fields = lookup_fields;
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
