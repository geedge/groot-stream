package com.geedgenetworks.common.config;

import lombok.Data;
import java.io.Serializable;
import java.util.Map;

@Data
public class Accumulator implements Serializable {
    private Map<String, Object> metricsFields;
    private long errorCount;
    private long inEvents;
    private long outEvents;

}
