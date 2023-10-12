package com.geedgenetworks.udf;

import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.pojo.ProjectionFunction;
import com.geedgenetworks.utils.JsonPathUtil;

public class JsonExtract implements Udf {

    private ProjectionFunction projection_function;

    @Override
    public void withParameters(ProjectionFunction projection_function) {
        this.projection_function=projection_function;
    }

    @Override
    public Event evaluate(Event event) {
        if(event.getExtractedFields().containsKey(projection_function.getLookup_fields().get(0))) {

            String result = (String) JsonPathUtil.analysis(event.getExtractedFields().get(projection_function.getLookup_fields().get(0)).toString(), projection_function.getParameters().get("param").toString());
            event.getExtractedFields().put(projection_function.getOutput_fields().get(0), result);
        }
        return event;
    }

    @Override
    public String functionName() {
        return "JSON_EXTRACT";
    }
}
