package com.geedgenetworks.core.udf;

import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;

public class Rename implements UDF {
    private UDFContext udfContext;

    @Override
    public void init (UDFContext udfContext) {
        this.udfContext = udfContext;
    }

    @Override
    public Event evaluate(Event event) {
        if (event.getExtractedFields().containsKey(udfContext.getLookup_fields().get(0))) {

            event.getExtractedFields()
                    .put(
                            udfContext.getOutput_fields().get(0),
                            event.getExtractedFields()
                                    .get(udfContext.getLookup_fields().get(0)));
            event.getExtractedFields().remove(udfContext.getLookup_fields().get(0));
        }
        return event;
    }

    @Override
    public String functionName() {
        return "RENAME";
    }
}
