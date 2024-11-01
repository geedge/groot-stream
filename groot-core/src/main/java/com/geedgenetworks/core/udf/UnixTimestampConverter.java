package com.geedgenetworks.core.udf;

import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.udf.ScalarFunction;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.udf.UDFContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.time.Instant;

@Slf4j
public class UnixTimestampConverter implements ScalarFunction {

    private UDFContext udfContext;
    private String precision;
    private String lookupFieldName;
    private String outputFieldName;

    private long interval;
    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {

        this.udfContext = udfContext;
        if(udfContext.getLookupFields()==null || udfContext.getOutputFields()==null || udfContext.getParameters() == null){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Missing required parameters");
        }
        if(udfContext.getOutputFields().size() != 1){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function output fields only support 1 value");
        }
        if(udfContext.getLookupFields().size() != 1){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function output fields only support 1 value");
        }
        if(!udfContext.getParameters().containsKey("precision")){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "parameters must containkey precision");
        }
        else{
            if(!udfContext.getParameters().get("precision").toString().equals("seconds") &&
                    !udfContext.getParameters().get("precision").toString().equals("milliseconds") &&
                        !udfContext.getParameters().get("precision").toString().equals("minutes")){
                throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "parameters option value is not correct");
            }else {

                this.precision =udfContext.getParameters().get("precision").toString();
            }
        }
        if(!udfContext.getParameters().containsKey("interval")){
            this.interval = 1L;
        }
        else{
            this.interval = Long.parseLong(udfContext.getParameters().get("interval").toString());
        }
        this.lookupFieldName = udfContext.getLookupFields().get(0);
        this.outputFieldName = udfContext.getOutputFields().get(0);

    }


    @Override
    public Event evaluate(Event event) {
        Object valueObj = event.getExtractedFields().get(lookupFieldName);
        if (valueObj!=null) {
            Long timestamp = Long.parseLong(valueObj.toString());
            Instant instant ;
            if (String.valueOf(timestamp).length() > 10) {
                instant = Instant.ofEpochMilli(timestamp);
            } else {
                instant = Instant.ofEpochSecond(timestamp);
            }
            switch (precision) {
                case "seconds":
                    timestamp = instant.getEpochSecond()/interval*interval;
                    break;
                case "milliseconds":
                    timestamp = instant.toEpochMilli()/interval*interval;
                    break;
                case "minutes":
                    timestamp = instant.getEpochSecond()/(interval*60)*(interval*60);
                    break;
            }
            event.getExtractedFields().put(outputFieldName, timestamp);
        }
        return event;
    }


    @Override
    public String functionName() {
        return "UNIX_TIMESTAMP_CONVERTER";
    }

    @Override
    public void close() {

    }
}
