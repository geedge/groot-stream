package com.geedgenetworks.core.pojo;

import java.io.Serializable;
import java.util.Map;

public class SinkConfig implements Serializable {
    private String type;
    private Map<String, String> properties;
    private String name;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
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
