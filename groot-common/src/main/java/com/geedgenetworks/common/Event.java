package com.geedgenetworks.common;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
public class Event implements Serializable {
    public static final String INTERNAL_TIMESTAMP_KEY = "__timestamp";
    public static final String INTERNAL_HEADERS_KEY = "__headers";
    public static final String WINDOW_START_TIMESTAMP = "__window_start_timestamp";
    public static final String WINDOW_END_TIMESTAMP = "__window_end_timestamp";

    private Map<String, Object> extractedFields;
    //Dropped flag, default is false. if set to true, indicates whether an event has been intentionally excluded and removed from further processing.
    private boolean isDropped = false;

}
