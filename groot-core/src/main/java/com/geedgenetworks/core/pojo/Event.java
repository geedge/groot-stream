package com.geedgenetworks.core.pojo;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
public class Event implements Serializable {
    public static final String INTERNAL_TIMESTAMP_KEY = "__timestamp";
    private Map<String, Object> extractedFields;
    // Event final flag, default is false. if set to true, stops ingestion data to the downstream functions and don't output next processor or sink.
    private boolean isFinal = false;


    public Map<String, Object> getExtractedFields() {
        return extractedFields;
    }

    public void setExtractedFields(Map<String, Object> extractedFields) {
        this.extractedFields = extractedFields;
    }

}
