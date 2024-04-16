package com.geedgenetworks.core.udf;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.udf.UDF;
import com.geedgenetworks.common.udf.UDFContext;
import com.geedgenetworks.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.io.UnsupportedEncodingException;
import java.util.Base64;

@Slf4j
public class EncodeBase64 implements UDF {

    private String valueField;
    private String outputFieldName;
    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {

        if(udfContext.getParameters()==null || udfContext.getOutput_fields()==null){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Missing required parameters");
        }
        if(udfContext.getOutput_fields().size() != 1){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function output fields only support 1 value");
        }
        if(!udfContext.getParameters().containsKey("value_field") ){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "parameters must containkey value_field ");
        }
        this.outputFieldName = udfContext.getOutput_fields().get(0);
        this.valueField =udfContext.getParameters().get("value_field").toString();



    }

    @Override
    public Event evaluate(Event event) {
        String encodeResult = "";
        if (event.getExtractedFields().containsKey(valueField)) {
            try {
                encodeResult = Base64.getEncoder().encodeToString((byte[]) event.getExtractedFields().getOrDefault(valueField,"".getBytes()));
            }  catch (RuntimeException e) {
            log.error("Encode Base64 exception, exception information:" + e.getMessage());
        }

            event.getExtractedFields()
                    .put(outputFieldName, encodeResult);
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
