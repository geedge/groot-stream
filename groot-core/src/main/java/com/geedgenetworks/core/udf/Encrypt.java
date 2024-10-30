package com.geedgenetworks.core.udf;

import cn.hutool.core.util.URLUtil;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.alibaba.fastjson2.JSON;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.config.CommonConfig;
import com.geedgenetworks.common.config.KmsConfig;
import com.geedgenetworks.common.config.SSLConfig;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.udf.ScalarFunction;
import com.geedgenetworks.common.udf.UDFContext;
import com.geedgenetworks.core.pojo.KmsKey;
import com.geedgenetworks.core.udf.encrypt.EncryptionAlgorithm;
import com.geedgenetworks.core.utils.*;
import com.geedgenetworks.shaded.org.apache.http.HttpHeaders;
import com.geedgenetworks.shaded.org.apache.http.HttpStatus;
import com.geedgenetworks.shaded.org.apache.http.message.BasicHeader;
import com.geedgenetworks.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class Encrypt implements ScalarFunction {

    private String lookupFieldName;
    private String outputFieldName;
    private String identifier;
    private String defaultVal;
    private String type;
    private transient SingleValueMap.Data<LoadIntervalDataUtil<Set<String>>> sensitiveFieldsData;
    private transient SingleValueMap.Data<LoadIntervalDataUtil<KmsKey>> kmsKeyData;
    private transient EncryptionAlgorithm encryptionAlgorithm;

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {
        checkUdfContext(udfContext);
        if (udfContext.getParameters().containsKey("default_val")) {
            this.defaultVal = udfContext.getParameters().get("default_val").toString();
        }
        this.lookupFieldName = udfContext.getLookupFields().get(0);
        this.outputFieldName = udfContext.getOutputFields().get(0);
        this.identifier = udfContext.getParameters().get("identifier").toString();
        Configuration configuration = (Configuration) runtimeContext.getExecutionConfig().getGlobalJobParameters();
        CommonConfig commonConfig = JSON.parseObject(configuration.toMap().get(Constants.SYSPROP_GROOTSTREAM_CONFIG), CommonConfig.class);
        KmsConfig kmsConfig = commonConfig.getKmsConfig().get(configuration.toMap().get(Constants.SYSPROP_KMS_TYPE_CONFIG));
        SSLConfig sslConfig = commonConfig.getSslConfig();
        Map<String, String> propertiesConfig = commonConfig.getPropertiesConfig();
        type = kmsConfig.getType();
        try {
            encryptionAlgorithm = EncryptionAlgorithmUtils.createEncryptionAlgorithm(identifier);
            if (encryptionAlgorithm == null) {
                throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Parameters identifier is illegal!");
            }
            if (!type.equals(KmsUtils.KMS_TYPE_LOCAL)) {
                kmsKeyData = SingleValueMap.acquireData("kmsKeyData",
                        () -> LoadIntervalDataUtil.newInstance(() -> KmsUtils.getVaultKey(kmsConfig, sslConfig, identifier),
                                LoadIntervalDataOptions.defaults("kmsKeyData", Integer.parseInt(propertiesConfig.getOrDefault(Constants.SYSPROP_ENCRYPT_KMS_KEY_SCHEDULER_INTERVAL_NAME, "5")) * 60000L)),
                        LoadIntervalDataUtil::stop);
                KmsKey kmsKey = kmsKeyData.getData().data();
                if (kmsKey == null) {
                    throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Initialization UDF Encrypt failed!");
                }
                if (encryptionAlgorithm.getSecretKeyLength() != kmsKey.getKeyData().length) {
                    throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Global parameter kms secret Key requires " + encryptionAlgorithm.getSecretKeyLength() + " bytes!");
                }
                encryptionAlgorithm.setKmsKey(kmsKey);
            }
            sensitiveFieldsData = SingleValueMap.acquireData("sensitiveFields",
                    () -> LoadIntervalDataUtil.newInstance(() -> getSensitiveFields(propertiesConfig.get("projection.encrypt.schema.registry.uri")),
                            LoadIntervalDataOptions.defaults("sensitiveFields", Integer.parseInt(propertiesConfig.getOrDefault(Constants.SYSPROP_ENCRYPT_SENSITIVE_FIELDS_SCHEDULER_INTERVAL_NAME, "5")) * 60000L)),
                    LoadIntervalDataUtil::stop);
            if (sensitiveFieldsData.getData().data() == null) {
                throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Initialization UDF Encrypt failed!");
            }
        } catch (Exception e) {
            throw new GrootStreamRuntimeException(CommonErrorCode.UNSUPPORTED_OPERATION, "Initialization UDF Encrypt failed!", e);
        }
    }

    @Override
    public Event evaluate(Event event) {
        try {
            if (!type.equals(KmsUtils.KMS_TYPE_LOCAL)) {
                KmsKey kmsKey = kmsKeyData.getData().data();
                if (kmsKey.getKeyVersion() != encryptionAlgorithm.getKmsKey().getKeyVersion() || !Arrays.equals(kmsKey.getKeyData(), encryptionAlgorithm.getKmsKey().getKeyData())) {
                    encryptionAlgorithm.setKmsKey(kmsKey);
                }
            }
            if (sensitiveFieldsData.getData().data().contains(lookupFieldName) && event.getExtractedFields().containsKey(lookupFieldName)) {
                String value = (String) event.getExtractedFields().get(lookupFieldName);
                if (StringUtil.isNotBlank(value)) {
                    String encryptResult = encryptionAlgorithm.encrypt(value);
                    if (StringUtil.isEmpty(encryptResult)) {
                        event.getExtractedFields().put(outputFieldName, StringUtil.isNotBlank(defaultVal) ? defaultVal : value);
                    } else {
                        if (KmsUtils.KMS_TYPE_VAULT.equals(type)) {
                            encryptResult = "vault:v" + encryptionAlgorithm.getKmsKey().getKeyVersion() + ":" + encryptResult;
                        }
                        event.getExtractedFields().put(outputFieldName, encryptResult);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return event;
    }

    @Override
    public String functionName() {
        return "ENCRYPT";
    }

    @Override
    public void close() {
        if (sensitiveFieldsData != null) {
            sensitiveFieldsData.release();
        }
        if (kmsKeyData != null) {
            kmsKeyData.release();
        }
    }

    private void checkUdfContext(UDFContext udfContext) {
        if (udfContext.getParameters() == null) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Missing required parameters");
        }
        if (udfContext.getLookupFields().size() != 1) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function lookup fields only support 1 value");
        }
        if (udfContext.getOutputFields().size() != 1) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function output fields only support 1 value");
        }
        if (!udfContext.getParameters().containsKey("identifier")) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Parameters must contains identifier");
        }
    }

    public Set<String> getSensitiveFields(String url) throws IOException {
        Set<String> sensitiveFieldsSet;
        String sensitiveFieldsStr = HttpClientPoolUtil.getInstance().httpGet(URI.create(URLUtil.normalize(url)), new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/x-www-form-urlencoded"));
        JSONObject sensitiveFieldsJson = JSONUtil.parseObj(sensitiveFieldsStr);
        if (sensitiveFieldsJson.getInt("status", HttpStatus.SC_INTERNAL_SERVER_ERROR) == HttpStatus.SC_OK) {
            JSONArray sensitiveFieldsJsonArr = sensitiveFieldsJson.getJSONArray("data");
            sensitiveFieldsSet = IntStream.range(0, sensitiveFieldsJsonArr.size())
                    .mapToObj(sensitiveFieldsJsonArr::getStr)
                    .collect(Collectors.toSet());
        } else {
            throw new GrootStreamRuntimeException(CommonErrorCode.UNSUPPORTED_OPERATION, "Get encrypt fields error! Error message: " + sensitiveFieldsStr);
        }
        return sensitiveFieldsSet;
    }
}
