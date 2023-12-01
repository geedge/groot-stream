package com.geedgenetworks.core.pojo;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class SourceConfigOld implements Serializable {

    private String name;
    private int parallelism;
    private Map<String, Object> properties;
    private String watermark_timestamp;
    private Long watermark_lag;
    private String type;
    private String format;
    private List<String> output_fields;

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

    public String getWatermark_timestamp() {
        return watermark_timestamp;
    }

    public void setWatermark_timestamp(String watermark_timestamp) {
        this.watermark_timestamp = watermark_timestamp;
    }

    public Long getWatermark_lag() {
        return watermark_lag;
    }

    public void setWatermark_lag(Long watermark_lag) {
        this.watermark_lag = watermark_lag;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public List<String> getOutput_fields() {
        return output_fields;
    }

    public void setOutput_fields(List<String> output_fields) {
        this.output_fields = output_fields;
    }
}
