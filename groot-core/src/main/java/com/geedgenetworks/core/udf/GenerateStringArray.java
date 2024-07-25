package com.geedgenetworks.core.udf;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.udf.ScalarFunction;
import com.geedgenetworks.common.udf.UDFContext;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.util.*;
import java.util.stream.Collectors;

public class GenerateStringArray implements ScalarFunction {

    private List<String> lookupFieldNames;
    private String outputFieldName;

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {

        if(udfContext.getLookup_fields()==null || udfContext.getOutput_fields()==null ){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Missing required parameters");
        }
        if(udfContext.getOutput_fields().size() != 1){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function output fields only support 1 value");
        }
        this.lookupFieldNames = udfContext.getLookup_fields();
        this.outputFieldName = udfContext.getOutput_fields().get(0);
    }

    @Override
    public Event evaluate(Event event) {

        List<String> list = new ArrayList<>();
        lookupFieldNames.forEach(fieldName -> {
            Object value = event.getExtractedFields().get(fieldName);
            if (value instanceof Collection) {
                list.addAll(((Collection<?>) value).stream().map(Object::toString).collect(Collectors.toList()));
            }
            else if (value instanceof String) {
                list.add(value.toString());
            }
            else if (value instanceof Number) {
                list.add(value.toString());
            }
            else if (value instanceof Object[]) {
                list.addAll(Arrays.stream((Object[]) value).map(Object::toString).collect(Collectors.toList()));
            }
        });
        event.getExtractedFields().put(outputFieldName, list);

        return event;
    }

    @Override
    public String functionName() {
        return "GENERATE_STRING_ARRAY";
    }

    @Override
    public void close() {

    }
}
