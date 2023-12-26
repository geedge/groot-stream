package com.geedgenetworks.core.udf;

import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;
import com.geedgenetworks.common.utils.JsonPathUtil;
import org.apache.flink.api.common.functions.RuntimeContext;

public class JsonExtract implements UDF {

    private UDFContext udfContext;
    private String lookupFieldName;
    private String outputFieldName;

    private String param;
    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {
        this.udfContext = udfContext;
        if(udfContext.getLookup_fields().size() != 1){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function lookup fields only support 1 value");
        }
        if(udfContext.getOutput_fields().size() != 1){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function output fields only support 1 value");
        }
        if(udfContext.getParameters() == null){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function must contain parameters");
        }
        if(!udfContext.getParameters().containsKey("param")){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "parameters must containkey param");
        }
        this.lookupFieldName = udfContext.getLookup_fields().get(0);
        this.outputFieldName = udfContext.getOutput_fields().get(0);
        this.param =udfContext.getParameters().get("param").toString();
    }




    @Override
    public Event evaluate(Event event) {
        if (event.getExtractedFields().containsKey(lookupFieldName)) {

            String result =
                    (String)
                            JsonPathUtil.analysis(
                                    event.getExtractedFields()
                                            .get(lookupFieldName)
                                            .toString(),param);
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
