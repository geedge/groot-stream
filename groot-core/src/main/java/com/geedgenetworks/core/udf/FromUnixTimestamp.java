package com.geedgenetworks.core.udf;

import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.api.common.udf.ScalarFunction;
import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.connector.event.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.TimeZone;
@Slf4j
public class FromUnixTimestamp implements ScalarFunction {
    private String precision;
    private String outputFieldName;
    private String lookupFieldName;

    private String timeZone = "UTC";

    private SimpleDateFormat sdf;
    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {

        if(udfContext.getLookupFields()==null || udfContext.getOutputFields()==null || udfContext.getParameters() == null){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Missing required parameters");
        }

        if(udfContext.getOutputFields().size() != 1){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function output fields only support 1 value");
        }
        if(udfContext.getLookupFields().size() != 1){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function lookup fields only support 1 value");
        }
        if(!udfContext.getParameters().containsKey("precision")){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "parameters must containkey precision");
        }
        else{
            if (!Arrays.asList("seconds", "milliseconds").contains(udfContext.getParameters().get("precision").toString().trim())) {
                throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "parameters precision value is not correct");
            }
        }
        if(udfContext.getParameters().containsKey("timezone")){
            timeZone = udfContext.getParameters().get("timezone").toString();
        }

        this.precision = udfContext.getParameters().get("precision").toString();
        this.outputFieldName = udfContext.getOutputFields().get(0);
        this.lookupFieldName = udfContext.getLookupFields().get(0);
        this.sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        switch (precision) {
            case "seconds":
                sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                break;
            case "milliseconds":
                sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                break;
            default:
                break;
        }
        sdf.setTimeZone(TimeZone.getTimeZone(timeZone));
    }

    @Override
    public Event evaluate(Event event) {

        Object objectValue = event.getExtractedFields().get(lookupFieldName);
               if(objectValue!=null){
                   String value = objectValue.toString();
                   String dateTimeFormat ="";
                   try {
                       long timestamp = Long.parseLong(value);
                       dateTimeFormat = sdf.format(timestamp >= 10000000000L ? timestamp : timestamp * 1000);
                   } catch (NumberFormatException e) {
                       log.error("Invalid timestamp format for field {}: {}", lookupFieldName, value, e);
                   }
                   event.getExtractedFields().put(outputFieldName, dateTimeFormat);
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
