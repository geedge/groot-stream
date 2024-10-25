package com.geedgenetworks.core.udf;

import cn.hutool.core.util.ArrayUtil;
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
import com.geedgenetworks.utils.StringUtil;
import io.github.jopenlibs.vault.VaultException;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

import java.util.Arrays;
import java.util.Map;

@Slf4j
public class Encrypt implements ScalarFunction {

    private String lookupFieldName;
    private String outputFieldName;
    private String identifier;
    private String defaultVal;
    private String type;
    private transient SingleValueMap.Data<LoadIntervalDataUtil<String[]>> sensitiveFieldsData;
    private transient SingleValueMap.Data<LoadIntervalDataUtil<KmsKey>> kmsKeyData;
    private transient EncryptionAlgorithm encryptionAlgorithm;

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {
        checkUdfContext(udfContext);
        if (udfContext.getParameters().containsKey("default_val")) {
            this.defaultVal = udfContext.getParameters().get("default_val").toString();
        }
        this.lookupFieldName = udfContext.getLookup_fields().get(0);
        this.outputFieldName = udfContext.getOutput_fields().get(0);
        this.identifier = udfContext.getParameters().get("identifier").toString();

        Configuration configuration = (Configuration) runtimeContext.getExecutionConfig().getGlobalJobParameters();
        CommonConfig commonConfig = JSON.parseObject(configuration.toMap().get(Constants.SYSPROP_GROOTSTREAM_CONFIG), CommonConfig.class);
        Map<String, KmsConfig> kmsConfigs = commonConfig.getKmsConfig();
        if (kmsConfigs.isEmpty()) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Global parameter kms type is not null!");
        } else if (kmsConfigs.size() > 1) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Global parameter kms type is repeated!");
        }
        KmsConfig kmsConfig = kmsConfigs.values().iterator().next();
        SSLConfig sslConfig = commonConfig.getSslConfig();
        Map<String, String> propertiesConfig = commonConfig.getPropertiesConfig();
        type = kmsConfig.getType();
        try {
            encryptionAlgorithm = EncryptionAlgorithmUtils.getEncryptionAlgorithm(identifier);
            if (encryptionAlgorithm == null) {
                throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Parameters identifier is illegal!");
            }
            kmsKeyData = SingleValueMap.acquireData("kmsKeyData",
                    () -> LoadIntervalDataUtil.newInstance(() -> getKmsKey(kmsConfig, sslConfig, identifier),
                            LoadIntervalDataOptions.defaults("kmsKeyData", 60000)), LoadIntervalDataUtil::stop);
            sensitiveFieldsData = SingleValueMap.acquireData("sensitiveFields",
                    () -> LoadIntervalDataUtil.newInstance(() -> getEncryptFields(propertiesConfig.get("projection.encrypt.schema.registry.uri")),
                            LoadIntervalDataOptions.defaults("sensitiveFields", 60000)), LoadIntervalDataUtil::stop);
            KmsKey kmsKey = kmsKeyData.getData().data();
            if (encryptionAlgorithm.getSecretKeyLength() == kmsKey.getKeyData().length) {
                encryptionAlgorithm.setKmsKey(kmsKey);
            } else {
                throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Global parameter kms secret Key requires " + encryptionAlgorithm.getSecretKeyLength() + " bytes!");
            }
        } catch (Exception e) {
            throw new GrootStreamRuntimeException(CommonErrorCode.UNSUPPORTED_OPERATION, "Initialization UDF Encrypt failed!", e);
        }
    }

    @Override
    public Event evaluate(Event event) {
        try {
            KmsKey kmsKey = kmsKeyData.getData().data();
            if (kmsKey.getKeyVersion() != encryptionAlgorithm.getKmsKey().getKeyVersion() || !Arrays.equals(kmsKey.getKeyData(), encryptionAlgorithm.getKmsKey().getKeyData())) {
                encryptionAlgorithm.setKmsKey(kmsKey);
            }
            if (ArrayUtil.contains(sensitiveFieldsData.getData().data(), lookupFieldName) && event.getExtractedFields().containsKey(lookupFieldName)) {
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
        if (udfContext.getLookup_fields().size() != 1) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function lookup fields only support 1 value");
        }
        if (udfContext.getOutput_fields().size() != 1) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function output fields only support 1 value");
        }
        if (!udfContext.getParameters().containsKey("identifier")) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Parameters must contains identifier");
        }
    }

    private KmsKey getKmsKey(KmsConfig kmsConfig, SSLConfig sslConfig, String identifier) throws VaultException {
        KmsKey kmsKey = null;
        if (KmsUtils.KMS_TYPE_VAULT.equals(kmsConfig.getType())) {
            kmsKey = KmsUtils.getVaultKey(kmsConfig, sslConfig, identifier);
        } else if (KmsUtils.KMS_TYPE_LOCAL.equals(kmsConfig.getType())) {
            kmsKey = new KmsKey(kmsConfig.getSecretKey().getBytes(), 1);
        }
        return kmsKey;
    }

    private String[] getEncryptFields(String url) {
        String[] encryptFields = new String[]{"phone_number", "server_ip"};
//        try {
//            String s = HttpClientPoolUtil.getInstance().httpGet(URI.create(URLUtil.normalize(url)));
//            encryptFields = s.split(",");
//        } catch (Exception e) {
//            log.error("Get encrypt fields error! " + e.getMessage());
//        }
        return encryptFields;
    }
}
