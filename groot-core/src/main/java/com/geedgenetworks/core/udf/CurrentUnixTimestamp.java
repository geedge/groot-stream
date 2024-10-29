package com.geedgenetworks.core.udf;

import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.udf.ScalarFunction;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.udf.UDFContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;

@Slf4j
public class CurrentUnixTimestamp implements ScalarFunction {

    private String precision;
    private String outputFieldName;

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {


        if( udfContext.getOutputFields()==null || udfContext.getParameters() == null){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Missing required parameters");
        }
        if(udfContext.getOutputFields().size() != 1){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function output fields only support 1 value");
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
        this.precision = udfContext.getParameters().get("precision").toString();
        this.outputFieldName = udfContext.getOutputFields().get(0);

    }

    @Override
    public Event evaluate(Event event) {
        long timestamp = System.currentTimeMillis();
        if ("seconds".equals(precision)) {
            timestamp = timestamp / 1000;
        }
        event.getExtractedFields().put(outputFieldName, timestamp);
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
