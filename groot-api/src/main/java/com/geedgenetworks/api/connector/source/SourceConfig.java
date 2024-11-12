package com.geedgenetworks.api.connector.source;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;
@Data
public class SourceConfig implements Serializable {
    private String name;
    private String type;
    private Map<String, Object> schema;
    private String watermark_timestamp;
    private String watermark_timestamp_unit = "ms";
    private Long watermark_lag;
    private Map<String, String> properties;
}
