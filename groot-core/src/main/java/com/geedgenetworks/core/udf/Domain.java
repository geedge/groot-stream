package com.geedgenetworks.core.udf;

import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;

public class Domain implements UDF {

    private UDFContext udfContext;

    @Override
    public void init(UDFContext udfContext) {
        this.udfContext = udfContext;
    }

    @Override
    public Event evaluate(Event event) {
        String Domain = "";

        if (event.getExtractedFields().containsKey(udfContext.getLookup_fields().get(0))) {
            Domain =
                    (String)
                            event.getExtractedFields()
                                    .get(udfContext.getLookup_fields().get(0));
        } else if (event.getExtractedFields()
                .containsKey(udfContext.getLookup_fields().get(1))) {
            Domain =
                    (String)
                            event.getExtractedFields()
                                    .get(udfContext.getLookup_fields().get(1));
        } else if (event.getExtractedFields()
                .containsKey(udfContext.getLookup_fields().get(2))) {
            Domain =
                    (String)
                            event.getExtractedFields()
                                    .get(udfContext.getLookup_fields().get(2));
        }
        event.getExtractedFields().put(udfContext.getOutput_fields().get(0), Domain);
        return event;
    }

    @Override
    public String functionName() {
        return "DOMAIN";
    }
}
