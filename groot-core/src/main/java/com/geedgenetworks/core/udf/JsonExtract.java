package com.geedgenetworks.core.udf;

import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;
import com.geedgenetworks.common.utils.JsonPathUtil;

public class JsonExtract implements UDF {

    private UDFContext udfContext;

    @Override
    public void init(UDFContext udfContext) {
        this.udfContext = udfContext;
    }

    @Override
    public Event evaluate(Event event) {
        if (event.getExtractedFields().containsKey(udfContext.getLookup_fields().get(0))) {

            String result =
                    (String)
                            JsonPathUtil.analysis(
                                    event.getExtractedFields()
                                            .get(udfContext.getLookup_fields().get(0))
                                            .toString(),
                                    udfContext.getParameters().get("param").toString());
            event.getExtractedFields().put(udfContext.getOutput_fields().get(0), result);
        }
        return event;
    }

    @Override
    public String functionName() {
        return "JSON_EXTRACT";
    }

    @Override
    public void close() {

    }
}
