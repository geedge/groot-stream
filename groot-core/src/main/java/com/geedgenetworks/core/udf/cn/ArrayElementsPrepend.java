package com.geedgenetworks.core.udf.cn;

import com.geedgenetworks.api.common.udf.ScalarFunction;
import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.event.Event;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/1/23 11:14
 */
public class ArrayElementsPrepend implements ScalarFunction {

    private String prefix;

    private String lookupFieldName;
    private String outputFieldName;

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {
        this.prefix = udfContext.getParameters().get("prefix").toString();
        this.lookupFieldName = udfContext.getLookupFields().get(0);
        this.outputFieldName = udfContext.getOutputFields().get(0);
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
