package com.geedgenetworks.udf;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.pojo.ProjectionFunction;
import com.geedgenetworks.utils.StringUtil;

import java.io.UnsupportedEncodingException;
import java.util.Base64;

public class DecodeBase64 implements Udf {

    private static final Log logger = LogFactory.get();
    private  ProjectionFunction projection_function;


    @Override
    public void withParameters(ProjectionFunction projection_function) {
        this.projection_function=projection_function;
    }

    @Override
    public Event evaluate(Event event) {
        if (event.getExtractedFields().containsKey(projection_function.getLookup_fields().get(0))) {

            String decodeResult = "";
            String message = (String) event.getExtractedFields().get(projection_function.getLookup_fields().get(0));
            Object charset = event.getExtractedFields().get(projection_function.getLookup_fields().get(1));
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
                logger.error("The Character Encoding [" + charset.toString() + "] is not supported.exception information:" + e.getMessage());
            }
            event.getExtractedFields().put(projection_function.getOutput_fields().get(0), decodeResult);
        }
            return event;
    }

    @Override
    public String functionName() {
        return "BASE64_DECODE_TO_STRING";
    }
}
