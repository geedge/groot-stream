package com.geedgenetworks.core.udf;

import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.spi.common.udf.ScalarFunction;
import com.geedgenetworks.spi.common.udf.UDFContext;
import com.geedgenetworks.spi.table.event.Event;
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

        if(udfContext.getLookupFields()==null || udfContext.getOutputFields()==null || udfContext.getParameters() == null){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Missing required parameters");
        }
        if(udfContext.getOutputFields().size() != 1){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function output fields only support 1 value");
        }
        this.lookupFieldNames = udfContext.getLookupFields();
        this.outputFieldName = udfContext.getOutputFields().get(0);
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
