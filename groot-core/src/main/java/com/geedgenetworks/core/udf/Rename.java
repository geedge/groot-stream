package com.geedgenetworks.core.udf;

import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;
import org.apache.flink.api.common.functions.RuntimeContext;

public class Rename implements UDF {
    private String lookupFieldName;
    private String outputFieldName;
    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {

        this.lookupFieldName = udfContext.getLookup_fields().get(0);
        this.outputFieldName = udfContext.getOutput_fields().get(0);
    }

    @Override
    public Event evaluate(Event event) {
        if (event.getExtractedFields().containsKey(lookupFieldName)) {

            event.getExtractedFields()
                    .put(outputFieldName,
                            event.getExtractedFields()
                                    .get(lookupFieldName));
            event.getExtractedFields().remove(lookupFieldName);
        }
        return event;
    }

    @Override
    public String functionName() {
        return "RENAME";
    }

    @Override
    public void close() {

    }
}
