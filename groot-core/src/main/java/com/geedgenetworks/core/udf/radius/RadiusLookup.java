package com.geedgenetworks.core.udf.radius;

import com.geedgenetworks.common.config.commonConfig;
import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;
import com.geedgenetworks.core.udf.UDF;

import org.apache.flink.configuration.Configuration;

public class RadiusLookup implements UDF {

    private transient RadiusLookupUtil radiusLookup;
    private UDFContext udfContext;

    @Override
    public void init(UDFContext udfContext) {
        this.radiusLookup = new RadiusLookupUtil(new Configuration());
        this.udfContext = udfContext;
    }

    @Override
    public Event evaluate(Event event) {

        if ("vsys".equals(commonConfig.DATA_RELATIONSHIP_MODEL)) {
            int vsysId =
                    Integer.parseInt(event.getExtractedFields().get("common_vsys_id").toString());
            String value = radiusLookup.get(udfContext.getLookup_fields().get(0) + vsysId);
            event.getExtractedFields().put(udfContext.getOutput_fields().get(0), value);
        } else {
            String value = radiusLookup.get(udfContext.getLookup_fields().get(0));
            event.getExtractedFields().put(udfContext.getOutput_fields().get(0), value);
        }
        return event;
    }

    @Override
    public String functionName() {
        return "RADIUS_LOOKUP";
    }
}
