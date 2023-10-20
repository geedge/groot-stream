package com.geedgenetworks.udf.radius;

import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.pojo.ProjectionFunction;
import com.geedgenetworks.udf.Udf;

import org.apache.flink.configuration.Configuration;

import static com.geedgenetworks.config.commonConfig.DATA_RELATIONSHIP_MODEL;

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

        if ("vsys".equals(DATA_RELATIONSHIP_MODEL)) {
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
