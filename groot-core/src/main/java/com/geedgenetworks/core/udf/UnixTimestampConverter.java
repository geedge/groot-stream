package com.geedgenetworks.core.udf;

import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;
import com.geedgenetworks.utils.FormatUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.time.Instant;

@Slf4j
public class UnixTimestampConverter implements UDF {

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
            }else {

                this.precision =udfContext.getParameters().get("precision").toString();
            }
        }


    }


    @Override
    public Event evaluate(Event event) {

        Long timestamp = Long.parseLong(event.getExtractedFields().get(udfContext.getLookup_fields().get(0)).toString());
        Instant instant = null;
        if (String.valueOf(timestamp).length() ==13 ) {
            // 时间戳长度大于10，表示为毫秒级时间戳
            instant = Instant.ofEpochMilli(timestamp);
        } else if(String.valueOf(timestamp).length() ==10){
            // 时间戳长度小于等于10，表示为秒级时间戳
            instant = Instant.ofEpochSecond(timestamp);
        }else {
            return event;
        }
        switch (precision) {
            case "seconds":
                timestamp = instant.getEpochSecond();
                break;
            case "milliseconds":
                timestamp = instant.toEpochMilli();
                break;
        }
        event.getExtractedFields().put(udfContext.getOutput_fields().get(0), timestamp);
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
