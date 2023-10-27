package com.geedgenetworks.core.udf;

import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;
import com.geedgenetworks.utils.FormatUtils;

public class FirstSignificantSubdomain implements UDF {

    private UDFContext udfContext;

    @Override
    public void init(UDFContext udfContext) {
        this.udfContext = udfContext;
    }

    @Override
    public Event evaluate(Event event) {

        String Subdomain = "";
        for (String lookupField : udfContext.getLookup_fields()){

            if(event.getExtractedFields().containsKey(lookupField)){

                Subdomain =
                        FormatUtils.getTopPrivateDomain(
                                (String)
                                        event.getExtractedFields()
                                                .get(lookupField));
                break;
            }
        }
        event.getExtractedFields().put(udfContext.getOutput_fields().get(0), Subdomain);
        return event;
    }



    @Override
    public String functionName() {
        return "FIRST_SIGNIFICANT_SUBDOMAIN";
    }
}
