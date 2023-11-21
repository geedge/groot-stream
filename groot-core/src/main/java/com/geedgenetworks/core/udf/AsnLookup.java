package com.geedgenetworks.core.udf;

import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;
import com.geedgenetworks.common.utils.IPUtils;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;

public class AsnLookup implements UDF {

    private UDFContext udfContext;
    private static final Log logger = LogFactory.get();
    private String vender;

    @Override
    public void init(UDFContext udfContext) {
        this.udfContext = udfContext;
        this.vender = udfContext.getParameters().get("kb_vender").toString();
    }

    @Override
    public Event evaluate(Event event) {
        if (event.getExtractedFields().containsKey(udfContext.getLookup_fields().get(0))) {
            event.getExtractedFields()
                    .put(
                            udfContext.getOutput_fields().get(0),
                            IPUtils.venderWithAsnLookup
                                    .get(vender)
                                    .asnLookup(
                                            event.getExtractedFields()
                                                    .get(
                                                            udfContext
                                                                    .getLookup_fields()
                                                                    .get(0))
                                                    .toString()));
        }
        return event;
    }

    @Override
    public String functionName() {
        return "ASN_LOOKUP";
    }

    @Override
    public void close() {

    }
}
