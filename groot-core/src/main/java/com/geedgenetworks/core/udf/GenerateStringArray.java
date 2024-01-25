package com.geedgenetworks.core.udf;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.udf.UDF;
import com.geedgenetworks.common.udf.UDFContext;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.util.ArrayList;
import java.util.List;

public class GenerateStringArray implements UDF {

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

        List<String> resultList = new ArrayList<>();
        for(String lookupFieldName :lookupFieldNames){

            resultList.add(event.getExtractedFields().getOrDefault(lookupFieldName,"").toString().trim());
        }
        event.getExtractedFields().put(outputFieldName,resultList);
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
