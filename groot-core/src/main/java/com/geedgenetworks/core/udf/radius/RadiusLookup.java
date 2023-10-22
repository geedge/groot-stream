package com.geedgenetworks.core.udf.radius;

import com.geedgenetworks.core.config.commonConfig;
import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.ProjectionFunction;
import com.geedgenetworks.core.udf.Udf;

import org.apache.flink.configuration.Configuration;

public class RadiusLookup implements Udf {

    private transient RadiusLookupUtil radiusLookup;
    private ProjectionFunction projectionFunction;

    @Override
    public void withParameters(ProjectionFunction projection_function) {
        this.radiusLookup = new RadiusLookupUtil(new Configuration());
        this.projectionFunction = projectionFunction;
    }

    @Override
    public Event evaluate(Event event) {

        if ("vsys".equals(commonConfig.DATA_RELATIONSHIP_MODEL)) {
            int vsysId =
                    Integer.parseInt(event.getExtractedFields().get("common_vsys_id").toString());
            String value = radiusLookup.get(projectionFunction.getLookup_fields().get(0) + vsysId);
            event.getExtractedFields().put(projectionFunction.getOutput_fields().get(0), value);
        } else {
            String value = radiusLookup.get(projectionFunction.getLookup_fields().get(0));
            event.getExtractedFields().put(projectionFunction.getOutput_fields().get(0), value);
        }
        return event;
    }

    @Override
    public String functionName() {
        return "RADIUS_LOOKUP";
    }
}
