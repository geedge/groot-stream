package com.geedgenetworks.core.udf;

import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;
import com.geedgenetworks.common.utils.JsonPathUtil;
import org.apache.flink.api.common.functions.RuntimeContext;

public class JsonExtract implements UDF {

    private String lookupFieldName;
    private String outputFieldName;
    private String expression;

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {
        if(udfContext.getLookup_fields().size() != 1){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function lookup fields only support 1 value");
        }
        if(udfContext.getOutput_fields().size() != 1){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function output fields only support 1 value");
        }
        if(udfContext.getParameters() == null){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function must contain parameters");
        }
        if(!udfContext.getParameters().containsKey("value_expression")){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "parameters must containkey value_expression");
        }
        this.lookupFieldName = udfContext.getLookup_fields().get(0);
        this.outputFieldName = udfContext.getOutput_fields().get(0);
        this.expression =udfContext.getParameters().get("value_expression").toString();
    }




    @Override
    public Event evaluate(Event event) {
        if (event.getExtractedFields().containsKey(lookupFieldName)) {

            String result =
                    (String)
                            JsonPathUtil.analysis(
                                    event.getExtractedFields()
                                            .get(lookupFieldName)
                                            .toString(),expression);
            event.getExtractedFields().put(outputFieldName, result);
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
