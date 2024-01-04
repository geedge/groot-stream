package com.geedgenetworks.core.pojo;

import com.geedgenetworks.core.types.StructType;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class SourceConfig implements Serializable {
    private String type;
    private List<Object> fields;
    private StructType dataType;
    private Map<String, String> properties;
    private String name;
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public List<Object> getFields() {
        return fields;
    }

    public void setFields(List<Object> fields) {
        this.fields = fields;
    }

    public StructType getDataType() {
        return dataType;
    }

    public void setDataType(StructType dataType) {
        this.dataType = dataType;
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
