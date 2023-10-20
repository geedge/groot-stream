package com.geedgenetworks.udf;

import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.pojo.ProjectionFunction;
import com.geedgenetworks.utils.IPUtils;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;

public class AsnLookup implements Udf {

    private ProjectionFunction projection_function;
    private static final Log logger = LogFactory.get();
    private String vender;

    @Override
    public void withParameters(ProjectionFunction projection_function) {
        this.projection_function = projection_function;
        this.vender = projection_function.getParameters().get("kb_vender").toString();
    }

    @Override
    public Event evaluate(Event event) {
        if (event.getExtractedFields().containsKey(projection_function.getLookup_fields().get(0))) {
            event.getExtractedFields()
                    .put(
                            projection_function.getOutput_fields().get(0),
                            IPUtils.venderWithAsnLookup
                                    .get(vender)
                                    .asnLookup(
                                            event.getExtractedFields()
                                                    .get(
                                                            projection_function
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
}
