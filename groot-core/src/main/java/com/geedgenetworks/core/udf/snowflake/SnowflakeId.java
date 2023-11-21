package com.geedgenetworks.core.udf.snowflake;

import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;
import com.geedgenetworks.core.udf.UDF;

import java.io.Serializable;

public class SnowflakeId implements Serializable, UDF {

    private UDFContext udfContext;

    @Override
    public void init(UDFContext udfContext) {
        this.udfContext = udfContext;
    }

    @Override
    public Event evaluate(Event event) {

        event.getExtractedFields()
                .put(udfContext.getOutput_fields().get(0), SnowflakeIdUtil.generateId());
        return event;
    }

    @Override
    public String functionName() {
        return "SNOWFLAKE_ID";
    }

    @Override
    public void close() {

    }
}
