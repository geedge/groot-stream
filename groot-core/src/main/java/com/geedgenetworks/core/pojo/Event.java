package com.geedgenetworks.core.pojo;

import java.io.Serializable;
import java.util.Map;

public class Event implements Serializable {

    private Map<String, Object> extractedFields;
    private Map<String, Object> internalFields;

    private String __inputId;

    private long __timestamp;

    private boolean isFinal;

    public Map<String, Object> getExtractedFields() {
        return extractedFields;
    }

    public void setExtractedFields(Map<String, Object> extractedFields) {
        this.extractedFields = extractedFields;
    }

    public Map<String, Object> getInternalFields() {
        return internalFields;
    }

    public void setInternalFields(Map<String, Object> internalFields) {
        this.internalFields = internalFields;
    }

    public String get__inputId() {
        return __inputId;
    }

    public void set__inputId(String __inputId) {
        this.__inputId = __inputId;
    }

    public long get__timestamp() {
        return __timestamp;
    }

    public void set__timestamp(long __timestamp) {
        this.__timestamp = __timestamp;
    }

    public boolean isFinal() {
        return isFinal;
    }

    public void setFinal(boolean aFinal) {
        isFinal = aFinal;
    }
}
