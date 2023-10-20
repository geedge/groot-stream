package com.geedgenetworks.pojo;

import java.io.Serializable;
import java.util.Map;

public class YamlEntity implements Serializable {

    private Map<String, Object> sources;
    private Map<String, Object> application;
    private Map<String, Object> sinks;
    private Map<String, Object> filters;
    private Map<String, Object> pre_processing_pipelines;
    private Map<String, Object> processing_pipelines;
    private Map<String, Object> post_processing_pipelines;

    public Map<String, Object> getSources() {
        return sources;
    }

    public void setSources(Map<String, Object> sources) {
        this.sources = sources;
    }

    public Map<String, Object> getApplication() {
        return application;
    }

    public void setApplication(Map<String, Object> application) {
        this.application = application;
    }

    public Map<String, Object> getSinks() {
        return sinks;
    }

    public void setSinks(Map<String, Object> sinks) {
        this.sinks = sinks;
    }

    public Map<String, Object> getFilters() {
        return filters;
    }

    public void setFilters(Map<String, Object> filters) {
        this.filters = filters;
    }

    public Map<String, Object> getPre_processing_pipelines() {
        return pre_processing_pipelines;
    }

    public void setPre_processing_pipelines(Map<String, Object> pre_processing_pipelines) {
        this.pre_processing_pipelines = pre_processing_pipelines;
    }

    public Map<String, Object> getProcessing_pipelines() {
        return processing_pipelines;
    }

    public void setProcessing_pipelines(Map<String, Object> processing_pipelines) {
        this.processing_pipelines = processing_pipelines;
    }

    public Map<String, Object> getPost_processing_pipelines() {
        return post_processing_pipelines;
    }

    public void setPost_processing_pipelines(Map<String, Object> post_processing_pipelines) {
        this.post_processing_pipelines = post_processing_pipelines;
    }
}
