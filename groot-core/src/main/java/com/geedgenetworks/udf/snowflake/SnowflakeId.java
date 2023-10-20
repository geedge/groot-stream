package com.geedgenetworks.udf.snowflake;

import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.pojo.ProjectionFunction;
import com.geedgenetworks.udf.Udf;

import java.io.Serializable;

public class SnowflakeId implements Serializable, Udf {

    private ProjectionFunction projection_function;

    @Override
    public void withParameters(ProjectionFunction projection_function) {
        this.projection_function = projection_function;
    }

    @Override
    public Event evaluate(Event event) {

        event.getExtractedFields()
                .put(projection_function.getOutput_fields().get(0), SnowflakeIdUtil.generateId());
        return event;
    }

    @Override
    public String functionName() {
        return "SNOWFLAKE_ID";
    }
}
