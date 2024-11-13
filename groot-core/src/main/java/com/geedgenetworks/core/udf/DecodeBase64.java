package com.geedgenetworks.core.udf;

import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.api.common.udf.ScalarFunction;
import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.connector.event.Event;
import com.geedgenetworks.utils.StringUtil;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.io.UnsupportedEncodingException;
import java.util.Base64;

@Slf4j
public class DecodeBase64 implements ScalarFunction {

    private String valueField;
    private String charsetField;
    private String outputFieldName;
    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {

        if(udfContext.getParameters()==null || udfContext.getOutputFields()==null){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Missing required parameters");
        }
        if(udfContext.getOutputFields().size() != 1){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function output fields only support 1 value");
        }
        if(!udfContext.getParameters().containsKey("value_field") ||!udfContext.getParameters().containsKey("charset_field") ){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "parameters must containkey value_field and charset_field");
        }
        this.outputFieldName = udfContext.getOutputFields().get(0);
        this.valueField =udfContext.getParameters().get("value_field").toString();
        this.charsetField =udfContext.getParameters().get("charset_field").toString();



    }

    @Override
    public Event evaluate(Event event) {
        Object valueObj = event.getExtractedFields().get(valueField);
        if (valueObj!=null) {
            String decodeResult = "";
            String message = (String)valueObj;
            Object charset = event.getExtractedFields().getOrDefault(charsetField,"UTF-8");
            try {
                if (StringUtil.isNotBlank(message)) {
                    byte[] base64decodedBytes = Base64.getDecoder().decode(message);
                    if (charset == null || "".equals(charset)) {
                        decodeResult = new String(base64decodedBytes);
                    } else {
                        decodeResult = new String(base64decodedBytes, charset.toString());
                    }
                }
            } catch (RuntimeException e) {
                log.error("Resolve Base64 exception, exception information:" + e.getMessage());
            } catch (UnsupportedEncodingException e) {
                log.error("The Character Encoding [" + charset.toString() + "] is not supported.exception information:" + e.getMessage());
            }
            event.getExtractedFields().put(outputFieldName, decodeResult);
        }
        return event;
    }

    @Override
    public String functionName() {
        return "BASE64_DECODE_TO_STRING";
    }

    @Override
    public void close() {

    }



}
