package com.geedgenetworks.core.pojo;

import com.geedgenetworks.core.types.StructType;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class SourceConfig implements Serializable {
    private String type;
    private Map<String, Object> schema;
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
