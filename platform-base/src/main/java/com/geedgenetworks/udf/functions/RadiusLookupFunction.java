package com.geedgenetworks.udf.functions;

import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.pojo.ProjectionFunction;
import com.geedgenetworks.udf.UdfFunction;
import com.geedgenetworks.udf.lookup.RadiusLookup;
import org.apache.flink.configuration.Configuration;

import static com.geedgenetworks.config.commonConfig.DATA_RELATIONSHIP_MODEL;

public class RadiusLookupFunction implements UdfFunction {

    private final transient RadiusLookup radiusLookup;
    private final ProjectionFunction projectionFunction;

    public RadiusLookupFunction(ProjectionFunction projectionFunction) {
        this.radiusLookup = new RadiusLookup(new Configuration());
        this.projectionFunction = projectionFunction;
    }

    @Override
    public void udffunction(Event event) {
        if ("vsys".equals(DATA_RELATIONSHIP_MODEL)) {
            int vsysId = Integer.parseInt(event.getExtractedFields().get("common_vsys_id").toString());
            String value = radiusLookup.get(projectionFunction.getLookup_fields().get(0) + vsysId);
            event.getExtractedFields().put(projectionFunction.getOutput_fields().get(0), value);
        } else {
            String value = radiusLookup.get(projectionFunction.getLookup_fields().get(0));
            event.getExtractedFields().put(projectionFunction.getOutput_fields().get(0), value);
        }
    }
}
