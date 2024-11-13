package com.geedgenetworks.core.udf;

import cn.hutool.core.util.URLUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.alibaba.fastjson2.JSON;
import com.geedgenetworks.common.config.Constants;
import com.geedgenetworks.common.config.*;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.utils.HttpClientPoolUtil;
import com.geedgenetworks.core.pojo.DataEncryptionKey;
import com.geedgenetworks.core.udf.encrypt.Crypto;
import com.geedgenetworks.core.utils.*;
import com.geedgenetworks.shaded.org.apache.http.HttpHeaders;
import com.geedgenetworks.shaded.org.apache.http.HttpStatus;
import com.geedgenetworks.shaded.org.apache.http.message.BasicHeader;
import com.geedgenetworks.api.common.udf.ScalarFunction;
import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.configuration.CheckUDFContextUtil;
import com.geedgenetworks.api.configuration.UDFContextConfigOptions;
import com.geedgenetworks.api.configuration.util.LoadIntervalDataOptions;
import com.geedgenetworks.api.configuration.util.LoadIntervalDataUtil;
import com.geedgenetworks.api.connector.event.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class Encrypt implements ScalarFunction {

    private String lookupFieldName;
    private String outputFieldName;
    private String identifier;
    private String defaultVal;
    private String kmsType;
    private transient SingleValueMap.Data<LoadIntervalDataUtil<Set<String>>> sensitiveFieldState;
    private transient SingleValueMap.Data<LoadIntervalDataUtil<DataEncryptionKey>> dekState;
    private transient Crypto crypto;

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {
        checkConfig(udfContext);
        configureParameters(udfContext);
        initializeCrypto(runtimeContext);
    }

    private void configureParameters(UDFContext udfContext) {
        this.defaultVal = Optional.ofNullable(udfContext.getParameters().get("default_val"))
                .map(Object::toString).orElse("");
        this.lookupFieldName = udfContext.getLookupFields().get(0);
        this.outputFieldName = udfContext.getOutputFields().get(0);
        this.identifier = udfContext.getParameters().get(UDFContextConfigOptions.PARAMETERS_IDENTIFIER.key()).toString();
    }

    private void initializeCrypto(RuntimeContext runtimeContext) {
        Configuration configuration = (Configuration) runtimeContext.getExecutionConfig().getGlobalJobParameters();
        CommonConfig commonConfig = JSON.parseObject(configuration.toMap().get(Constants.SYSPROP_GROOTSTREAM_CONFIG), CommonConfig.class);
        KmsConfig kmsConfig = commonConfig.getKmsConfig().get(configuration.toMap().get(Constants.SYSPROP_KMS_TYPE_CONFIG));
        ClientSSLConfig sslConfig = commonConfig.getSslConfig();
        Map<String, String> propertiesConfig = commonConfig.getPropertiesConfig();
        this.kmsType = kmsConfig.getType();
        try {
            this.crypto = CryptoProvider.createEncryptionAlgorithm(identifier);
            initializeDataEncryptionKeyIfNeeded(kmsConfig, sslConfig, propertiesConfig);
            initializeSensitiveFieldsIfNeeded(propertiesConfig);
        } catch (Exception e) {
            throw new GrootStreamRuntimeException(CommonErrorCode.UNSUPPORTED_OPERATION, "Initialization UDF Encrypt failed!", e);
        }
    }

    private void initializeDataEncryptionKeyIfNeeded(KmsConfig kmsConfig, ClientSSLConfig sslConfig, Map<String, String> propertiesConfig) throws Exception {
        if (KMSUtils.KMS_TYPE_VAULT.equals(kmsType)) {
            this.dekState = SingleValueMap.acquireData("dekState",
                    () -> LoadIntervalDataUtil.newInstance(
                            () -> KMSUtils.getVaultKey(kmsConfig, sslConfig, identifier),
                            LoadIntervalDataOptions.defaults("dekState",
                                    Integer.parseInt(propertiesConfig.getOrDefault(Constants.SYSPROP_ENCRYPT_KMS_KEY_SCHEDULER_INTERVAL_NAME, "1")) * 60000L)),
                    LoadIntervalDataUtil::stop);

            DataEncryptionKey dek = dekState.getData().data();

            if (dek == null) {
                throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, String.format("URI: %s, Unable to get KMS Secret Key!", kmsConfig.getUrl()));
            }

            if (crypto.getSecretKeyLength() != dek.getData().length) {
                throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, String.format("Version: %s, [%s] KMS Secret Key requires %s bytes!",
                        dek.getVersion(), crypto.getIdentifier(), crypto.getSecretKeyLength()));
            }

            crypto.setDataEncryptionKey(dek);
        }
    }

    private void initializeSensitiveFieldsIfNeeded(Map<String, String> propertiesConfig) throws Exception {
        String schemaUri = propertiesConfig.get("projection.encrypt.schema.registry.uri");
        if (schemaUri != null) {
            this.sensitiveFieldState = SingleValueMap.acquireData("sensitiveFieldState",
                    () -> LoadIntervalDataUtil.newInstance(
                            () -> getSensitiveFields(schemaUri),
                            LoadIntervalDataOptions.defaults("sensitiveFieldState",
                                    Integer.parseInt(propertiesConfig.getOrDefault(Constants.SYSPROP_ENCRYPT_SENSITIVE_FIELDS_SCHEDULER_INTERVAL_NAME, "1")) * 60000L)),
                    LoadIntervalDataUtil::stop);
            if (sensitiveFieldState.getData().data() == null) {
                throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, String.format("URI: %s, Unable to get encrypted fields!", schemaUri));
            }
        }
    }


    @Override
    public Event evaluate(Event event) {
        try {
            refreshDataEncryptionKey();

            if (isSensitiveField(lookupFieldName)) {
                executeEncrypt(event);
            }
        } catch (Exception e) {
            log.error("Encrypt failed!", e);
        }
        return event;
    }

    private void refreshDataEncryptionKey() throws Exception {
        if (dekState == null || dekState.getData().data() == null) {
            return;
        }
        DataEncryptionKey dek = dekState.getData().data();
        if (!Arrays.equals(dek.getData(), crypto.getDataEncryptionKey().getData())) {
            crypto.setDataEncryptionKey(dek);
        }

    }

    private boolean isSensitiveField(String fieldName) throws Exception {
        return sensitiveFieldState == null || sensitiveFieldState.getData().data().contains(fieldName);
    }

    private void executeEncrypt(Event event) {
        Object value = event.getExtractedFields().get(lookupFieldName);
        if (value != null) {
            String encryptResult = Optional.ofNullable(crypto.encrypt(value.toString())).orElse(defaultVal);
            if (KMSUtils.KMS_TYPE_VAULT.equals(kmsType)) {
                encryptResult = "vault:v" + crypto.getDataEncryptionKey().getVersion() + ":" + encryptResult;
            }
            event.getExtractedFields().put(outputFieldName, encryptResult);
        }
    }

    @Override
    public String functionName() {
        return "ENCRYPT";
    }

    @Override
    public void close() {
        if (sensitiveFieldState != null) {
            sensitiveFieldState.release();
        }
        if (dekState != null) {
            dekState.release();
        }
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

        result = CheckUDFContextUtil.checkCollectionSingleItemExists(udfContext, UDFContextConfigOptions.LOOKUP_FIELDS.key());
        if (!result.isSuccess()) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, result.getMsg());
        }

        result = CheckUDFContextUtil.checkCollectionSingleItemExists(udfContext, UDFContextConfigOptions.OUTPUT_FIELDS.key());
        if (!result.isSuccess()) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, result.getMsg());
        }

        result = CheckUDFContextUtil.checkParametersContainsKeys(udfContext, UDFContextConfigOptions.PARAMETERS_IDENTIFIER.key());
        if (!result.isSuccess()) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, result.getMsg());
        }


    }

    // If accessing the schema URI fails, the last known state of sensitiveFieldState will be used
    private Set<String> getSensitiveFields(String url) throws Exception {
        try {
            String result = HttpClientPoolUtil.getInstance()
                    .httpGet(URI.create(URLUtil.normalize(url)), new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/x-www-form-urlencoded"));
            JSONObject resultJson = JSONUtil.parseObj(result);
            int statusCode = resultJson.getInt("status", HttpStatus.SC_INTERNAL_SERVER_ERROR);
            if (statusCode == HttpStatus.SC_OK) {
                return Optional.ofNullable(resultJson.getJSONArray("data"))
                        .map(jsonArray -> IntStream.range(0, jsonArray.size())
                                .mapToObj(jsonArray::getStr)
                                .collect(Collectors.toSet()))
                        .orElseGet(Collections::emptySet);
            } else {
                log.error("Get sensitive fields error! Error message: {}", result);
                return sensitiveFieldState.getData().data();
            }
        } catch (Exception e) {
            log.error("Get sensitive fields error! Error message: {}", e.getMessage());
            return sensitiveFieldState.getData().data(); // return the previous value
        }
    }

}
