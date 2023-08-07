package com.geedgenetworks.udf.snowflake;

import com.geedgenetworks.pojo.ProjectionFunction;
import com.geedgenetworks.pojo.GrootEvent;
import com.geedgenetworks.udf.UdfFunction;

public class SnowflakeIdFunction implements UdfFunction {

    private ProjectionFunction projection_function;
    public SnowflakeIdFunction(ProjectionFunction projection_function) {
         this.projection_function=projection_function;
         new SnowflakeId(projection_function.getParameters().get("snowid_zookeeper").toString(), Long.parseLong( projection_function.getParameters().get("data_center_id_num").toString()));
    }

    @Override
    public void udffunction(GrootEvent grootEvent) {

        grootEvent.getAuxiliaryFieldsAndValues().put(projection_function.getOutput_fields().get(0), SnowflakeId.generateId());

    }



}
