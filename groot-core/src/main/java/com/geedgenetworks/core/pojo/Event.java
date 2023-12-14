package com.geedgenetworks.core.pojo;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
public class Event implements Serializable {
    public static final String INTERNAL_TIMESTAMP_KEY = "__timestamp";
    private Map<String, Object> extractedFields;
    //Dropped flag, default is false. if set to true, indicates whether an event has been intentionally excluded and removed from further processing.
    private boolean isDropped = false;
    
    public Map<String, Object> getExtractedFields() {
        return extractedFields;
    }

    public void setExtractedFields(Map<String, Object> extractedFields) {
        this.extractedFields = extractedFields;
    }

}
