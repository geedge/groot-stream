package com.geedgenetworks.udf.functions;

import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.pojo.ProjectionFunction;
import com.geedgenetworks.udf.UdfFunction;
import com.geedgenetworks.udf.lookup.GtpCLookup;
import org.apache.flink.configuration.Configuration;

public class GtpCLookupFunction implements UdfFunction {

    private final transient GtpCLookup gtpCLookup;
    private final ProjectionFunction projectionFunction;

    public GtpCLookupFunction(ProjectionFunction projectionFunction) {
        this.gtpCLookup = new GtpCLookup(new Configuration());
        this.projectionFunction = projectionFunction;
    }

    @Override
    public void udffunction(Event event) {
        GtpCLookup.GtpC gtpC = gtpCLookup.get(projectionFunction.getLookup_fields().get(0));
        if (gtpC != null) {
            event.getExtractedFields().put(projectionFunction.getOutput_fields().get(0), gtpC.phoneNumber);
            event.getExtractedFields().put(projectionFunction.getOutput_fields().get(1), gtpC.imsi);
            event.getExtractedFields().put(projectionFunction.getOutput_fields().get(2), gtpC.imei);
        }
    }
}
