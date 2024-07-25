package com.geedgenetworks.core.udf;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.udf.ScalarFunction;
import com.geedgenetworks.common.udf.UDFContext;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.util.List;

public class StringJoiner implements ScalarFunction {

    private List<String> lookupFieldNames;
    private String outputFieldName;
    private String separator;
    private String prefix ;
    private String suffix ;


    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {

        if(udfContext.getLookup_fields()==null || udfContext.getOutput_fields()==null || udfContext.getParameters() == null){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Missing required parameters");
        }
        if(udfContext.getOutput_fields().size() != 1){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function output fields only support 1 value");
        }
        this.lookupFieldNames = udfContext.getLookup_fields();
        this.outputFieldName = udfContext.getOutput_fields().get(0);
        this.separator =udfContext.getParameters().getOrDefault("separator","").toString().trim();
        this.prefix =udfContext.getParameters().getOrDefault("prefix","").toString().trim();
        this.suffix =udfContext.getParameters().getOrDefault("suffix","").toString().trim();

    }

    @Override
    public Event evaluate(Event event) {

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(prefix);
        for(String lookupFieldName :lookupFieldNames){

            stringBuilder.append(event.getExtractedFields().getOrDefault(lookupFieldName,"").toString().trim());
            stringBuilder.append(separator);
        }
        int lastCommaIndex = stringBuilder.lastIndexOf(separator);
        if (lastCommaIndex >= 0) {
            stringBuilder.deleteCharAt(lastCommaIndex);
        }
        stringBuilder.append(suffix);
        event.getExtractedFields().put(outputFieldName,stringBuilder.toString());
        return event;
    }

    @Override
    public String functionName() {
        return "STRING_JOINER";
    }

    @Override
    public void close() {

    }
}
