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
 * @date 2024/1/23 11:20
 */
@Deprecated
public class FieldsMerge implements ScalarFunction {

    private List<String> lookupFieldNames;
    private String outputFieldName;

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {
        this.lookupFieldNames = udfContext.getLookupFields();
        this.outputFieldName = udfContext.getOutputFields().get(0);
    }

    @Override
    public Event evaluate(Event event) {
        List<String> list = new ArrayList<>();
        lookupFieldNames.forEach(fieldName -> {
            Object value = event.getExtractedFields().get(fieldName);
            if (value instanceof ArrayList) {
                list.addAll(((ArrayList<?>) value).stream().map(Object::toString).collect(Collectors.toList()));
            }
            if (value instanceof String) {
                list.add((String) value);
            }
        });
        event.getExtractedFields().put(outputFieldName, list);
        return event;
    }

    @Override
    public String functionName() {
        return "CN_FIELDS_MERGE";
    }

    @Override
    public void close() {

    }
}
