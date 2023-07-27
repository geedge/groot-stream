package net.geedgenetworks.udf;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import net.geedgenetworks.pojo.GrootEvent;
import net.geedgenetworks.pojo.ProjectionFunction;
import com.zdjizhi.utils.StringUtil;

import java.io.UnsupportedEncodingException;
import java.util.Base64;
import java.util.Map;

public class DecodeBase64Function implements UdfFunction {

    private static final Log logger = LogFactory.get();
    private ProjectionFunction projection_function;
    public DecodeBase64Function(ProjectionFunction projection_function) {
        this.projection_function=projection_function;
    }


    private static Object isJsonValue(Map<String, Object> AuxiliaryFieldsAndValue, String param) {
        if (param.contains("$.")) {
            return AuxiliaryFieldsAndValue.get(param.substring(2));
        } else {
            return param;
        }
    }

    @Override
    public void udffunction(GrootEvent grootEvent) {
        String decodeResult = "";
        String message = (String) grootEvent.getAuxiliaryFieldsAndValues().get(projection_function.getLookup_fields().get(0));
        String param = (String) grootEvent.getAuxiliaryFieldsAndValues().get(projection_function.getLookup_fields().get(1));
        Object charset = isJsonValue(grootEvent.getAuxiliaryFieldsAndValues(), param);

        try {
            if (StringUtil.isNotBlank(message) ){
                byte[] base64decodedBytes = Base64.getDecoder().decode(message);
                if (charset == null) {
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
        grootEvent.getAuxiliaryFieldsAndValues().put(projection_function.getOutput_fields().get(0),decodeResult);
    }
}
