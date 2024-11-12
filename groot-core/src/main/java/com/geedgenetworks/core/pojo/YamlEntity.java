package com.geedgenetworks.core.pojo;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
public class YamlEntity implements Serializable {

    private Map<String, Object> sources;
    private Map<String, Object> application;
    private Map<String, Object> sinks;
    private Map<String, Object> filters;
    private Map<String, Object> pre_processing_pipelines;
    private Map<String, Object> processing_pipelines;
    private Map<String, Object> post_processing_pipelines;
}
