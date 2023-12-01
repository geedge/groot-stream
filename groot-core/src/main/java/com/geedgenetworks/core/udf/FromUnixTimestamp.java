package com.geedgenetworks.core.udf;

import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.text.SimpleDateFormat;
import java.util.TimeZone;
@Slf4j
public class FromUnixTimestamp implements UDF {
    private UDFContext udfContext;

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {
        this.udfContext = udfContext;
        if(udfContext.getOutput_fields().size() != 1){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function output fields only support 1 value");
        }
        if(udfContext.getLookup_fields().size() != 1){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function lookup fields only support 1 value");
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
                        !udfContext.getParameters().get("precision").toString().equals("microseconds") &&
                            !udfContext.getParameters().get("precision").toString().equals("nanoseconds") ){
                throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "parameters precision value is not correct");
            }
        }
    }

    @Override
    public Event evaluate(Event event) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
               switch (udfContext.getParameters().get("precision").toString()) {
                   case "seconds":
                       sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                       break;
                   case "milliseconds":
                       sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
                       break;
                   case "microseconds":
                       sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS:000");
                       break;
                   case "nanoseconds":
                       sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS:000:000");
                       break;
                   default:
                       break;
               }
               if(event.getExtractedFields().get(udfContext.getLookup_fields().get(0)).toString()!=null){
                   sdf.setTimeZone(TimeZone.getTimeZone(udfContext.getParameters().get("timezone").toString()));
                   String timestamp = sdf.format(Long.parseLong(event.getExtractedFields().get(udfContext.getLookup_fields().get(0)).toString()));
                   event.getExtractedFields().put(udfContext.getOutput_fields().get(0), timestamp);
               }else {
                   log.error("timestamp_field is null");
               }

               return  event;
    }

    @Override
    public String functionName() {
        return "FROM_UNIX_TIMESTAMP";
    }

    @Override
    public void close() {

    }
}
