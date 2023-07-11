package com.zdjizhi.base.pojo;

import java.util.LinkedHashMap;

public class YamlPojo {
    private  LinkedHashMap<String, Object> Sources;
    private  LinkedHashMap<String, Object> Sinks;
    private  LinkedHashMap<String, Object> Filter;
    private  LinkedHashMap<String, Object> Pre_processing_Pipelines;
    private  LinkedHashMap<String, Object> Processing_Pipelines;
    private  LinkedHashMap<String, Object> excuter;

    public LinkedHashMap<String, Object> getSources() {
        return Sources;
    }

    public void setSources(LinkedHashMap<String, Object> sources) {
        Sources = sources;
    }

    public LinkedHashMap<String, Object> getSinks() {
        return Sinks;
    }

    public void setSinks(LinkedHashMap<String, Object> sinks) {
        Sinks = sinks;
    }

    public LinkedHashMap<String, Object> getFilter() {
        return Filter;
    }

    public void setFilter(LinkedHashMap<String, Object> filter) {
        Filter = filter;
    }

    public LinkedHashMap<String, Object> getPre_processing_Pipelines() {
        return Pre_processing_Pipelines;
    }

    public void setPre_processing_Pipelines(LinkedHashMap<String, Object> pre_processing_Pipelines) {
        Pre_processing_Pipelines = pre_processing_Pipelines;
    }

    public LinkedHashMap<String, Object> getProcessing_Pipelines() {
        return Processing_Pipelines;
    }

    public void setProcessing_Pipelines(LinkedHashMap<String, Object> processing_Pipelines) {
        Processing_Pipelines = processing_Pipelines;
    }

    public LinkedHashMap<String, Object> getExcuter() {
        return excuter;
    }

    public void setExcuter(LinkedHashMap<String, Object> excuter) {
        this.excuter = excuter;
    }
}
