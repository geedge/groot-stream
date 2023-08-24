package com.geedgenetworks.udf;

import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.pojo.ProjectionFunction;
import com.zdjizhi.utils.FormatUtils;


public class FirstSignificantSubdomainFunction implements UdfFunction {


    private ProjectionFunction projection_function;
    public FirstSignificantSubdomainFunction(ProjectionFunction projection_function) {
        this.projection_function=projection_function;
    }

    @Override
    public void udffunction(Event event) {
        String  topDomain = "";

        if(event.getExtractedFields().containsKey(projection_function.getLookup_fields().get(0))){
            topDomain = FormatUtils.getTopPrivateDomain((String) event.getExtractedFields().get(projection_function.getLookup_fields().get(0)));
        }
        else if(event.getExtractedFields().containsKey(projection_function.getLookup_fields().get(1))){
            topDomain = FormatUtils.getTopPrivateDomain((String) event.getExtractedFields().get(projection_function.getLookup_fields().get(1)));
        }
        else if(event.getExtractedFields().containsKey(projection_function.getLookup_fields().get(2))){
            topDomain = FormatUtils.getTopPrivateDomain((String) event.getExtractedFields().get(projection_function.getLookup_fields().get(2)));
        }
        event.getExtractedFields().put(projection_function.getOutput_fields().get(0),topDomain);
    }
}
