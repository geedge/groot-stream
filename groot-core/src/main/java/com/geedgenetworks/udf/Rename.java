package com.geedgenetworks.udf;

import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.pojo.ProjectionFunction;

public class Rename implements Udf {
    private ProjectionFunction projection_function;

    @Override
    public void withParameters(ProjectionFunction projection_function) {
        this.projection_function = projection_function;
    }

    @Override
    public Event evaluate(Event event) {
        if (event.getExtractedFields().containsKey(projection_function.getLookup_fields().get(0))) {

            event.getExtractedFields()
                    .put(
                            projection_function.getOutput_fields().get(0),
                            event.getExtractedFields()
                                    .get(projection_function.getLookup_fields().get(0)));
            event.getExtractedFields().remove(projection_function.getLookup_fields().get(0));
        }
        return event;
    }

    @Override
    public String functionName() {
        return "RENAME";
    }
}
