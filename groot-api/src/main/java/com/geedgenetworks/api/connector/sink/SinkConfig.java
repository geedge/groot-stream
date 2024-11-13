package com.geedgenetworks.api.connector.sink;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;
@Data
public class SinkConfig implements Serializable {
    private String name;
    private String type;
    private Map<String, Object> schema;
    private Map<String, String> properties;

}
