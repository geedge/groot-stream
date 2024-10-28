package com.geedgenetworks.core.udf;

import cn.hutool.crypto.digest.HMac;
import cn.hutool.crypto.digest.HmacAlgorithm;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.udf.ScalarFunction;
import com.geedgenetworks.common.udf.UDFContext;
import com.geedgenetworks.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;

@Slf4j
public class Hmac implements ScalarFunction {

    private String lookupFieldName;
    private String outputFieldName;
    private String outputFormat;
    private HMac hMac;

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {
        checkUdfContext(udfContext);
        String secretKey = udfContext.getParameters().get("secret_key").toString();
        String algorithm = "sha256";
        if (udfContext.getParameters().containsKey("algorithm")) {
            algorithm = udfContext.getParameters().get("algorithm").toString();
        }
        this.hMac = new HMac(getHmacAlgorithm(algorithm), secretKey.getBytes());
        this.lookupFieldName = udfContext.getLookup_fields().get(0);
        this.outputFieldName = udfContext.getOutput_fields().get(0);
        this.outputFormat = "base64";
        if (udfContext.getParameters().containsKey("output_format")) {
            this.outputFormat = udfContext.getParameters().get("output_format").toString();
        }
    }

    @Override
    public Event evaluate(Event event) {
        String encodeResult = "";
        String message = (String) event.getExtractedFields().get(lookupFieldName);
        if (StringUtil.isNotBlank(message)) {
            switch (outputFormat) {
                case "hex":
                    encodeResult = hMac.digestHex(message);
                    break;
                case "base64":
                    encodeResult = hMac.digestBase64(message, false);
                    break;
                default:
                    encodeResult = hMac.digestBase64(message, false);
                    break;
            }
        }
        event.getExtractedFields().put(outputFieldName, encodeResult);
        return event;
    }

    @Override
    public String functionName() {
        return "HMAC";
    }

    @Override
    public void close() {

    }

    private void checkUdfContext(UDFContext udfContext) {
        if (udfContext.getParameters() == null || udfContext.getOutput_fields() == null) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Missing required parameters");
        }
        if (udfContext.getLookup_fields().size() != 1) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function lookup fields only support 1 value");
        }
        if (udfContext.getOutput_fields().size() != 1) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function output fields only support 1 value");
        }
        if (!udfContext.getParameters().containsKey("secret_key")) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "parameters must contains secret_key");
        }
    }

    private String getHmacAlgorithm(String algorithm) {
        if (StringUtil.containsIgnoreCase(algorithm, "sha256")) {
            return HmacAlgorithm.HmacSHA256.getValue();
        } else if (StringUtil.containsIgnoreCase(algorithm, "sha1")) {
            return HmacAlgorithm.HmacSHA1.getValue();
        } else if (StringUtil.containsIgnoreCase(algorithm, "md5")) {
            return HmacAlgorithm.HmacMD5.getValue();
        } else if (StringUtil.containsIgnoreCase(algorithm, "sha384")) {
            return HmacAlgorithm.HmacSHA384.getValue();
        } else if (StringUtil.containsIgnoreCase(algorithm, "sha512")) {
            return HmacAlgorithm.HmacSHA512.getValue();
        } else if (StringUtil.containsIgnoreCase(algorithm, "sm3")) {
            return HmacAlgorithm.HmacSM3.getValue();
        } else if (StringUtil.containsIgnoreCase(algorithm, "sm4")) {
            return HmacAlgorithm.SM4CMAC.getValue();
        } else {
            return HmacAlgorithm.HmacSHA256.getValue();
        }
    }
}
