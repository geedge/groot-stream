package com.geedgenetworks.udf.gtpc;

import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.pojo.ProjectionFunction;
import com.geedgenetworks.udf.Udf;

import org.apache.flink.configuration.Configuration;

public class GtpCLookup implements Udf {

    private transient GtpCLookupUtil gtpCLookupUtil;
    private ProjectionFunction projectionFunction;

    @Override
    public void withParameters(ProjectionFunction projection_function) {
        this.gtpCLookupUtil = new GtpCLookupUtil(new Configuration());
        this.projectionFunction = projection_function;
    }

    @Override
    public Event evaluate(Event event) {
        GtpCLookupUtil.GtpC gtpC = gtpCLookupUtil.get(projectionFunction.getLookup_fields().get(0));
        if (gtpC != null) {
            event.getExtractedFields()
                    .put(projectionFunction.getOutput_fields().get(0), gtpC.phoneNumber);
            event.getExtractedFields().put(projectionFunction.getOutput_fields().get(1), gtpC.imsi);
            event.getExtractedFields().put(projectionFunction.getOutput_fields().get(2), gtpC.imei);
        }
        return event;
    }

    @Override
    public String functionName() {
        return "GTP_C_LOOKUP";
    }
}
