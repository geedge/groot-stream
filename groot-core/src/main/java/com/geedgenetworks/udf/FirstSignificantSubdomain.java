package com.geedgenetworks.udf;

import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.pojo.ProjectionFunction;
import com.geedgenetworks.utils.FormatUtils;

public class FirstSignificantSubdomain implements Udf {

    private ProjectionFunction projection_function;

    @Override
    public void withParameters(ProjectionFunction projection_function) {
        this.projection_function = projection_function;
    }

    @Override
    public Event evaluate(Event event) {
        String topDomain = "";

        if (event.getExtractedFields().containsKey(projection_function.getLookup_fields().get(0))) {
            topDomain =
                    FormatUtils.getTopPrivateDomain(
                            (String)
                                    event.getExtractedFields()
                                            .get(projection_function.getLookup_fields().get(0)));
        } else if (event.getExtractedFields()
                .containsKey(projection_function.getLookup_fields().get(1))) {
            topDomain =
                    FormatUtils.getTopPrivateDomain(
                            (String)
                                    event.getExtractedFields()
                                            .get(projection_function.getLookup_fields().get(1)));
        } else if (event.getExtractedFields()
                .containsKey(projection_function.getLookup_fields().get(2))) {
            topDomain =
                    FormatUtils.getTopPrivateDomain(
                            (String)
                                    event.getExtractedFields()
                                            .get(projection_function.getLookup_fields().get(2)));
        }
        event.getExtractedFields().put(projection_function.getOutput_fields().get(0), topDomain);
        return event;
    }

    @Override
    public String functionName() {
        return "FIRST_SIGNIFICANT_SUBDOMAIN";
    }
}
