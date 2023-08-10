package com.geedgenetworks.udf.snowflake;

import com.geedgenetworks.pojo.ProjectionFunction;
import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.udf.UdfFunction;

import java.io.Serializable;

public class SnowflakeIdFunction implements Serializable,UdfFunction {

    private final ProjectionFunction projection_function;
    public SnowflakeIdFunction(ProjectionFunction projection_function) {
         this.projection_function=projection_function;
         new SnowflakeId(projection_function.getParameters().get("snowid_zookeeper").toString(), Long.parseLong( projection_function.getParameters().get("data_center_id_num").toString()));
    }

    @Override
    public void udffunction(Event event) {

        event.getExtractedFields().put(projection_function.getOutput_fields().get(0), SnowflakeId.generateId());

    }



}
