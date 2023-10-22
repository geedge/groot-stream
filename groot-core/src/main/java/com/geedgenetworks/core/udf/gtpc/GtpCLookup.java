package com.geedgenetworks.core.udf.gtpc;

import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;
import com.geedgenetworks.core.udf.UDF;

import org.apache.flink.configuration.Configuration;

public class GtpCLookup implements UDF {

    private transient GtpCLookupUtil gtpCLookupUtil;
    private UDFContext udfContext;

    @Override
    public void init(UDFContext udfContext) {
        this.gtpCLookupUtil = new GtpCLookupUtil(new Configuration());
        this.udfContext = udfContext;
    }

    @Override
    public Event evaluate(Event event) {
        GtpCLookupUtil.GtpC gtpC = gtpCLookupUtil.get(udfContext.getLookup_fields().get(0));
        if (gtpC != null) {
            event.getExtractedFields()
                    .put(udfContext.getOutput_fields().get(0), gtpC.phoneNumber);
            event.getExtractedFields().put(udfContext.getOutput_fields().get(1), gtpC.imsi);
            event.getExtractedFields().put(udfContext.getOutput_fields().get(2), gtpC.imei);
        }
        return event;
    }

    @Override
    public String functionName() {
        return "GTP_C_LOOKUP";
    }
}
