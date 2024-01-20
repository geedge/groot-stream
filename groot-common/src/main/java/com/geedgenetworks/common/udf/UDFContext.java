package com.geedgenetworks.common.udf;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class UDFContext implements Serializable {

    private String name;
    private List<String> lookup_fields;
    private List<String> output_fields;
    private String filter;
    private Map<String, Object> parameters;
    private String function;

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

    public String getFunction() {
        return function;
    }

    public void setFunction(String function) {
        this.function = function;
    }
}
