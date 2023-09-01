package com.geedgenetworks.udf;

import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.pojo.ProjectionFunction;

public class UnixTimestampFunction implements UdfFunction {


    private ProjectionFunction projection_function;
    public UnixTimestampFunction(ProjectionFunction projection_function) {
        this.projection_function=projection_function;
    }

    @Override
    public void udffunction(Event event) {

        long timestamp =  System.currentTimeMillis();
        if("seconds".equals(projection_function.getParameters().get("precision"))){
             timestamp =timestamp/1000;
        }
        event.getExtractedFields().put(projection_function.getOutput_fields().get(0),timestamp);


    }
}
