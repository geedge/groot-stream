package com.geedgenetworks.core.udf;

import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;
import com.geedgenetworks.common.utils.JsonPathUtil;
import org.apache.flink.api.common.functions.RuntimeContext;

public class JsonExtract implements UDF {

    private UDFContext udfContext;

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

    }




    @Override
    public Event evaluate(Event event) {
        if (event.getExtractedFields().containsKey(udfContext.getLookup_fields().get(0))) {

            String result =
                    (String)
                            JsonPathUtil.analysis(
                                    event.getExtractedFields()
                                            .get(udfContext.getLookup_fields().get(0))
                                            .toString(),
                                    udfContext.getParameters().get("param").toString());
            event.getExtractedFields().put(udfContext.getOutput_fields().get(0), result);
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
