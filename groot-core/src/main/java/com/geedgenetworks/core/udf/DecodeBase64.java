package com.geedgenetworks.core.udf;

import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;
import com.geedgenetworks.utils.StringUtil;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;

import java.io.UnsupportedEncodingException;
import java.util.Base64;

public class DecodeBase64 implements UDF {

    private static final Log logger = LogFactory.get();
    private UDFContext udfContext;

    @Override
    public void init(UDFContext udfContext) {
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
                    event.getExtractedFields().get(udfContext.getLookup_fields().get(1));
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
                logger.error("Resolve Base64 exception, exception information:" + e.getMessage());
            } catch (UnsupportedEncodingException e) {
                logger.error(
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
}
