package com.geedgenetworks.core.udf;

import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;
import com.geedgenetworks.utils.StringUtil;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.io.UnsupportedEncodingException;
import java.util.Base64;

@Slf4j
public class DecodeBase64 implements UDF {

    private UDFContext udfContext;

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {
        if(udfContext.getLookup_fields().size() !=2){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function lookup fields only support 1 value");
        }
        if(udfContext.getOutput_fields().size() != 1){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function output fields only support 1 value");
        }
        this.udfContext = udfContext;

    }

    @Override
    public Event evaluate(Event event) {
        if (event.getExtractedFields().containsKey(udfContext.getLookup_fields().get(0))) {

            String decodeResult = "";
            String message =
                    (String)
                            event.getExtractedFields()
                                    .get(udfContext.getLookup_fields().get(0));
            Object charset =
                    event.getExtractedFields().getOrDefault(udfContext.getLookup_fields().get(1),"");
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
                    .put(udfContext.getOutput_fields().get(0), decodeResult);
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
