package com.geedgenetworks.udf;

import com.geedgenetworks.pojo.ProjectionFunction;
import com.geedgenetworks.pojo.Event;

public class RenameFunction implements UdfFunction {
    private ProjectionFunction projection_function;

    public RenameFunction(ProjectionFunction projection_function) {
        this.projection_function = projection_function;
    }



    @Override
    public void udffunction(Event event) {

        event.getExtractedFields().put(projection_function.getOutput_fields().get(0), event.getExtractedFields().get(projection_function.getLookup_fields().get(0)));
        event.getExtractedFields().remove(projection_function.getLookup_fields().get(0));
    }
}
