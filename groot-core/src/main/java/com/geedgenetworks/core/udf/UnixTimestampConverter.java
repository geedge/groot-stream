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
        if(udfContext.getLookup_fields()==null || udfContext.getOutput_fields()==null || udfContext.getParameters() == null){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Missing required parameters");
        }
        if(udfContext.getOutput_fields().size() != 1){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function output fields only support 1 value");
        }
        if(udfContext.getLookup_fields().size() != 1){
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
        this.lookupFieldName = udfContext.getLookup_fields().get(0);
        this.outputFieldName = udfContext.getOutput_fields().get(0);

    }


    @Override
    public Event evaluate(Event event) {

        if(event.getExtractedFields().containsKey(lookupFieldName)) {
            Long timestamp = Long.parseLong(event.getExtractedFields().get(lookupFieldName).toString());
            Instant instant = null;
            if (String.valueOf(timestamp).length() == 13) {
                // 时间戳长度大于10，表示为毫秒级时间戳
                instant = Instant.ofEpochMilli(timestamp);
            } else if (String.valueOf(timestamp).length() == 10) {
                // 时间戳长度小于等于10，表示为秒级时间戳
                instant = Instant.ofEpochSecond(timestamp);
            } else {
                return event;
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
