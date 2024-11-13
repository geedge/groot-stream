package com.geedgenetworks.core.udf;

import cn.hutool.crypto.digest.HMac;
import cn.hutool.crypto.digest.HmacAlgorithm;
import com.geedgenetworks.common.config.CheckResult;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.api.common.udf.ScalarFunction;
import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.utils.CheckUDFContextUtil;
import com.geedgenetworks.api.common.udf.UDFContextConfigOptions;
import com.geedgenetworks.api.event.Event;
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
        checkConfig(udfContext);
        String secretKey = udfContext.getParameters().get("secret_key").toString();
        String algorithm = "sha256";
        if (udfContext.getParameters().containsKey("algorithm")) {
            algorithm = udfContext.getParameters().get("algorithm").toString();
        }
        this.hMac = new HMac(getHmacAlgorithm(algorithm), secretKey.getBytes());
        this.lookupFieldName = udfContext.getLookupFields().get(0);
        this.outputFieldName = udfContext.getOutputFields().get(0);
        this.outputFormat = "base64";
        if (udfContext.getParameters().containsKey("output_format")) {
            this.outputFormat = udfContext.getParameters().get("output_format").toString();
        }
    }

    @Override
    public Event evaluate(Event event) {
        String encodeResult = "";
        Object value = event.getExtractedFields().get(lookupFieldName);
        if (StringUtil.isNotEmpty(value)) {
            switch (outputFormat) {
                case "hex":
                    encodeResult = hMac.digestHex(value.toString());
                    break;
                case "base64":
                    encodeResult = hMac.digestBase64(value.toString(), false);
                    break;
                default:
                    encodeResult = hMac.digestBase64(value.toString(), false);
                    break;
            }
            event.getExtractedFields().put(outputFieldName, encodeResult);
        }

        return event;
    }

    @Override
    public String functionName() {
        return "HMAC";
    }

    @Override
    public void close() {

    }

    @Override
    public void checkConfig(UDFContext udfContext) {

        CheckResult result = CheckUDFContextUtil.checkAllExists(udfContext,
                UDFContextConfigOptions.LOOKUP_FIELDS.key(),
                UDFContextConfigOptions.OUTPUT_FIELDS.key(),
                UDFContextConfigOptions.PARAMETERS.key());

        if (!result.isSuccess()) {
            throw new GrootStreamRuntimeException(CommonErrorCode.CONFIG_VALIDATION_FAILED, result.getMsg());
        }
        result = CheckUDFContextUtil.checkParametersContainsKeys(udfContext, UDFContextConfigOptions.PARAMETERS_SECRET_KEY.key());
        if (!result.isSuccess()) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, result.getMsg());
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
