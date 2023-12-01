package com.geedgenetworks.core.pojo;

import java.io.Serializable;
import java.util.Map;

public class Event implements Serializable {
    public static final String INTERNAL_TIMESTAMP_KEY = "__timestamp";

    private Map<String, Object> extractedFields;

    private String __inputId;


    private boolean isFinal = true;

    public Map<String, Object> getExtractedFields() {
        return extractedFields;
    }

    public void setExtractedFields(Map<String, Object> extractedFields) {
        this.extractedFields = extractedFields;
    }


    public String get__inputId() {
        return __inputId;
    }

    public void set__inputId(String __inputId) {
        this.__inputId = __inputId;
    }

    public boolean isFinal() {
        return isFinal;
    }

    public void setFinal(boolean aFinal) {
        isFinal = aFinal;
    }
}
