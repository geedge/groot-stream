package com.geedgenetworks.pojo;

import java.io.Serializable;
import java.util.Map;

public class Event implements Serializable {

    private Map<String, Object> extractedFields;


    private Map<String, Object> header;

    private String id;

    private String key_by;

    private long timestamp;

    private boolean isFinal;

    public Map<String, Object> getExtractedFields() {
        return extractedFields;
    }

    public void setExtractedFields(Map<String, Object> extractedFields) {
        this.extractedFields = extractedFields;
    }

    public Map<String, Object> getHeader() {
        return header;
    }

    public void setHeader(Map<String, Object> header) {
        this.header = header;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getKey_by() {
        return key_by;
    }

    public void setKey_by(String key_by) {
        this.key_by = key_by;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public boolean isFinal() {
        return isFinal;
    }

    public void setFinal(boolean aFinal) {
        isFinal = aFinal;
    }
}
