package com.geedgenetworks.core.pojo;

import java.io.Serializable;
import java.util.Map;

public class SourceConfig implements Serializable {
    private String type;
    private Map<String, Object> schema;
    private String watermark_timestamp;
    private String watermark_timestamp_unit = "ms";
    private Long watermark_lag;
    private Map<String, String> properties;
    private String name;
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, Object> getSchema() {
        return schema;
    }

    public void setSchema(Map<String, Object> schema) {
        this.schema = schema;
    }

    public String getWatermark_timestamp() {
        return watermark_timestamp;
    }

    public void setWatermark_timestamp(String watermark_timestamp) {
        this.watermark_timestamp = watermark_timestamp;
    }

    public String getWatermark_timestamp_unit() {
        return watermark_timestamp_unit;
    }

    public void setWatermark_timestamp_unit(String watermark_timestamp_unit) {
        this.watermark_timestamp_unit = watermark_timestamp_unit;
    }

    public Long getWatermark_lag() {
        return watermark_lag;
    }

    public void setWatermark_lag(Long watermark_lag) {
        this.watermark_lag = watermark_lag;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
