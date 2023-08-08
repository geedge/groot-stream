package com.geedgenetworks.udf;

import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.pojo.ProjectionFunction;
import com.zdjizhi.utils.FormatUtils;

public class TopDomainFunction implements UdfFunction {


    private ProjectionFunction projection_function;
    public TopDomainFunction(ProjectionFunction projection_function) {
        this.projection_function=projection_function;
    }

    @Override
    public void udffunction(Event event) {
        String  topDomain = FormatUtils.getTopPrivateDomain((String) event.getExtractedFields().get(projection_function.getLookup_fields().get(0)));
        event.getExtractedFields().put(projection_function.getOutput_fields().get(0),topDomain);
    }
}
