package com.geedgenetworks.core.udf;

import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.api.common.udf.ScalarFunction;
import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.connector.event.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

@Slf4j
public class EncodeBase64 implements ScalarFunction {

    private String input_type;
    private String outputFieldName;
    private String lookupFieldName;
    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {

        if(udfContext.getParameters()==null || udfContext.getOutputFields()==null || udfContext.getLookupFields() == null){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Missing required parameters");
        }
        if(udfContext.getOutputFields().size() != 1){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function output fields only support 1 value");
        }
        if(udfContext.getLookupFields().size() != 1){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function lookup fields only support 1 value");
        }
        if(!udfContext.getParameters().containsKey("input_type") ){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "parameters must containkey value_field ");
        }
        this.outputFieldName = udfContext.getOutputFields().get(0);
        this.lookupFieldName = udfContext.getLookupFields().get(0);
        this.input_type =udfContext.getParameters().get("input_type").toString();

    }

    @Override
    public Event evaluate(Event event) {
        String encodeResult = "";
        Object valueObj = event.getExtractedFields().get(lookupFieldName);
        if (valueObj!=null) {
            try {
                switch (input_type) {
                    case "byte_array":
                        encodeResult = Base64.getEncoder().encodeToString((byte[]) valueObj);
                        break;
                    case "string":
                        encodeResult = Base64.getEncoder().encodeToString(valueObj.toString().getBytes(StandardCharsets.UTF_8));
                        break;
                    default:
                        log.error("Encode Base64 exception, Unsupport input_type :" + input_type);
                        break;
                }
            }  catch (RuntimeException e) {
            log.error("Encode Base64 exception, exception information:" + e.getMessage());
        }
            event.getExtractedFields().put(outputFieldName, encodeResult);
        }
        return event;
    }

    @Override
    public String functionName() {
        return "BASE64_ENCODE_TO_STRING";
    }

    @Override
    public void close() {

    }



}
