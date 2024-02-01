package com.geedgenetworks.core.udf.cn;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.udf.UDF;
import com.geedgenetworks.common.udf.UDFContext;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/1/23 11:14
 */
public class ArrayElementsPrepend implements UDF {

    private String prefix;

    private String lookupFieldName;
    private String outputFieldName;

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {
        this.prefix = udfContext.getParameters().get("prefix").toString();
        this.lookupFieldName = udfContext.getLookup_fields().get(0);
        this.outputFieldName = udfContext.getOutput_fields().get(0);
    }

    @Override
    public Event evaluate(Event event) {
        if (event.getExtractedFields().get(lookupFieldName) != null && event.getExtractedFields().get(lookupFieldName) instanceof ArrayList) {
            List<String> list = ((ArrayList<?>) (event.getExtractedFields().get(lookupFieldName))).stream().map(s -> prefix + s).collect(Collectors.toList());
            event.getExtractedFields().put(outputFieldName, list);
            return event;
        }
        return event;
    }

    @Override
    public String functionName() {
        return "CN_ARRAY_ELEMENTS_PREPEND";
    }

    @Override
    public void close() {

    }
}
