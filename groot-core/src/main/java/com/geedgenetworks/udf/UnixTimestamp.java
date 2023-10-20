package com.geedgenetworks.udf;

import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.pojo.ProjectionFunction;

public class UnixTimestamp implements Udf {

    private ProjectionFunction projection_function;

    @Override
    public void withParameters(ProjectionFunction projection_function) {
        this.projection_function = projection_function;
    }

    @Override
    public Event evaluate(Event event) {
        long timestamp = System.currentTimeMillis();
        if ("seconds".equals(projection_function.getParameters().get("precision"))) {
            timestamp = timestamp / 1000;
        }
        event.getExtractedFields().put(projection_function.getOutput_fields().get(0), timestamp);
        return event;
    }

    @Override
    public String functionName() {
        return "UNIX_TIMESTAMP_FUNCTION";
    }
}
