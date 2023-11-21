package com.geedgenetworks.core.udf;

import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;

public class UnixTimestamp implements UDF {

    private UDFContext udfContext;

    @Override
    public void init(UDFContext udfContext) {
        this.udfContext = udfContext;
    }

    @Override
    public Event evaluate(Event event) {
        long timestamp = System.currentTimeMillis();
        if ("seconds".equals(udfContext.getParameters().get("precision"))) {
            timestamp = timestamp / 1000;
        }
        event.getExtractedFields().put(udfContext.getOutput_fields().get(0), timestamp);
        return event;
    }

    @Override
    public String functionName() {
        return "UNIX_TIMESTAMP_FUNCTION";
    }

    @Override
    public void close() {

    }
}
