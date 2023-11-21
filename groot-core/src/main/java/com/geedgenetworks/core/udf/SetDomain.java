package com.geedgenetworks.core.udf;

import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;

public class SetDomain implements UDF {

    private UDFContext udfContext;

    @Override
    public void init(UDFContext udfContext) {
        this.udfContext = udfContext;
    }

    @Override
    public Event evaluate(Event event) {

        String domain = "";
        for (String lookupField : udfContext.getLookup_fields()){

            if(event.getExtractedFields().containsKey(lookupField)){

                domain = (String) event.getExtractedFields()
                                                .get(lookupField);
                break;
            }
        }
        event.getExtractedFields().put(udfContext.getOutput_fields().get(0), domain);
        return event;
    }

    @Override
    public String functionName() {
        return "SETDOMAIN";
    }

    @Override
    public void close() {

    }
}
