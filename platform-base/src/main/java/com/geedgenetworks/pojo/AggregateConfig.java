package com.geedgenetworks.pojo;

import java.util.List;
import java.util.Map;

public class AggregateConfig {

    private String type;
    private int parallelism;
    private List<String> group_by_fields;
    private String timestamp_field;
    private String window_type;
    private Integer window_interval_seconds;
    private Integer sliding_interval;
    private List<AggregateFunction> functions;
    private String[] output_fields;
    private Map<String, Object> properties;

    private String name;


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public List<String> getGroup_by_fields() {
        return group_by_fields;
    }

    public void setGroup_by_fields(List<String> group_by_fields) {
        this.group_by_fields = group_by_fields;
    }

    public String getTimestamp_field() {
        return timestamp_field;
    }

    public void setTimestamp_field(String timestamp_field) {
        this.timestamp_field = timestamp_field;
    }

    public String getWindow_type() {
        return window_type;
    }

    public void setWindow_type(String window_type) {
        this.window_type = window_type;
    }

    public Integer getWindow_interval_seconds() {
        return window_interval_seconds;
    }

    public void setWindow_interval_seconds(Integer window_interval_seconds) {
        this.window_interval_seconds = window_interval_seconds;
    }

    public Integer getSliding_interval() {
        return sliding_interval;
    }

    public void setSliding_interval(Integer sliding_interval) {
        this.sliding_interval = sliding_interval;
    }

    public List<AggregateFunction> getFunctions() {
        return functions;
    }

    public void setFunctions(List<AggregateFunction> functions) {
        this.functions = functions;
    }

    public String[] getOutput_fields() {
        return output_fields;
    }

    public void setOutput_fields(String[] output_fields) {
        this.output_fields = output_fields;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }
}
