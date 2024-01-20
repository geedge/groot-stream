package com.geedgenetworks.core.udf;

import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.udf.UDF;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.udf.UDFContext;
import com.geedgenetworks.utils.StringUtil;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.io.UnsupportedEncodingException;
import java.util.Base64;

@Slf4j
public class DecodeBase64 implements UDF {

    private String lookupFieldNameFirst;
    private String lookupFieldNameSecond;
    private String outputFieldName;
    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {

        if(udfContext.getLookup_fields()==null || udfContext.getOutput_fields()==null){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Missing required parameters");
        }
        if(udfContext.getLookup_fields().size() !=2){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function lookup fields only support 1 value");
        }
        if(udfContext.getOutput_fields().size() != 1){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function output fields only support 1 value");
        }
        this.lookupFieldNameFirst = udfContext.getLookup_fields().get(0);
        this.lookupFieldNameSecond = udfContext.getLookup_fields().get(1);
        this.outputFieldName = udfContext.getOutput_fields().get(0);
    }

    @Override
    public Event evaluate(Event event) {
        if (event.getExtractedFields().containsKey(lookupFieldNameFirst)) {

            String decodeResult = "";
            String message =
                    (String)
                            event.getExtractedFields()
                                    .get(lookupFieldNameFirst);
            Object charset =
                    event.getExtractedFields().getOrDefault(lookupFieldNameSecond,"");
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
                log.error(
                        "The Character Encoding ["
                                + charset.toString()
                                + "] is not supported.exception information:"
                                + e.getMessage());
            }
            event.getExtractedFields()
                    .put(outputFieldName, decodeResult);
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
