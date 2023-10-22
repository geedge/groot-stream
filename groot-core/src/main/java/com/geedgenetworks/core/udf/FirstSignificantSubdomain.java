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
        String topDomain = "";

        if (event.getExtractedFields().containsKey(udfContext.getLookup_fields().get(0))) {
            topDomain =
                    FormatUtils.getTopPrivateDomain(
                            (String)
                                    event.getExtractedFields()
                                            .get(udfContext.getLookup_fields().get(0)));
        } else if (event.getExtractedFields()
                .containsKey(udfContext.getLookup_fields().get(1))) {
            topDomain =
                    FormatUtils.getTopPrivateDomain(
                            (String)
                                    event.getExtractedFields()
                                            .get(udfContext.getLookup_fields().get(1)));
        } else if (event.getExtractedFields()
                .containsKey(udfContext.getLookup_fields().get(2))) {
            topDomain =
                    FormatUtils.getTopPrivateDomain(
                            (String)
                                    event.getExtractedFields()
                                            .get(udfContext.getLookup_fields().get(2)));
        }
        event.getExtractedFields().put(udfContext.getOutput_fields().get(0), topDomain);
        return event;
    }

    @Override
    public String functionName() {
        return "FIRST_SIGNIFICANT_SUBDOMAIN";
    }
}
