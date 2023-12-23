package com.geedgenetworks.core.udf;

import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;

@Slf4j
public class CurrentUnixTimestamp implements UDF {

    private UDFContext udfContext;

    private String precision;
    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {

        this.udfContext = udfContext;

        if(udfContext.getOutput_fields().size() != 1){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function output fields only support 1 value");
        }
        if(udfContext.getParameters() == null){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function must contain parameters");
        }
        if(!udfContext.getParameters().containsKey("precision")){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "parameters must containkey precision");
        }
        else{
            if(!udfContext.getParameters().get("precision").toString().equals("seconds") &&
                    !udfContext.getParameters().get("precision").toString().equals("milliseconds")){
                throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "parameters option value is not correct");
            }
        }


    }

    @Override
    public Event evaluate(Event event) {
        long timestamp = System.currentTimeMillis();
        if ("seconds".equals(udfContext.getParameters().get("precision"))) {
            timestamp = timestamp / 1000;
        }
        event.getExtractedFields().put(udfContext.getOutput_fields().get(0), timestamp);
        return event;

    }

    @Override
    public String functionName() {
        return "CURRENT_UNIX_TIMESTAMP";
    }

    @Override
    public void close() {

    }
}
