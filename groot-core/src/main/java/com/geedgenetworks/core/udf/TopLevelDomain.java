package com.geedgenetworks.core.udf;

import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;
import com.geedgenetworks.utils.FormatUtils;

public class TopLevelDomain implements UDF {

    private UDFContext udfContext;

    @Override
    public void init(UDFContext udfContext) {
        this.udfContext = udfContext;
    }

    @Override
    public Event evaluate(Event event) {
        String topDomain = "";
        for (String lookupField : udfContext.getLookup_fields()){

            if(event.getExtractedFields().containsKey(lookupField)){

                topDomain =
                        FormatUtils.getTopPrivateDomain(
                                (String)
                                        event.getExtractedFields()
                                                .get(lookupField));
                topDomain = topDomain.substring(topDomain.indexOf(".") + 1);
                break;
            }


        }
        event.getExtractedFields().put(udfContext.getOutput_fields().get(0), topDomain);
        return event;
    }

    @Override
    public String functionName() {
        return "TOP_LEVEL_DOMAIN";
    }

    @Override
    public void close() {

    }
}
