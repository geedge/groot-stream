package com.geedgenetworks.core.udf.test.simple;

import cn.hutool.core.util.RandomUtil;
import com.alibaba.fastjson2.JSON;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.config.CommonConfig;
import com.geedgenetworks.common.config.KmsConfig;
import com.geedgenetworks.common.config.ClientSSLConfig;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.udf.UDFContext;
import com.geedgenetworks.core.pojo.DataEncryptionKey;
import com.geedgenetworks.core.udf.Encrypt;
import com.geedgenetworks.core.udf.encrypt.Crypto;
import com.geedgenetworks.core.utils.CryptoProvider;
import com.geedgenetworks.core.utils.HttpClientPoolUtil;
import com.geedgenetworks.core.utils.KMSUtils;
import io.github.jopenlibs.vault.VaultException;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.*;

import java.io.IOException;
import java.security.Security;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class EncryptFunctionTest {
    private static UDFContext udfContext;
    private static MockedStatic<HttpClientPoolUtil> httpClientPoolUtilMockedStatic;
    private static final String DATA = "13812345678";

    @BeforeAll
    public static void setUp() throws IOException {
        Security.addProvider(new BouncyCastleProvider());
        udfContext = new UDFContext();
        udfContext.setLookupFields(Collections.singletonList("phone_number"));
        udfContext.setOutputFields(Collections.singletonList("phone_number"));
        httpClientPoolUtilMockedStatic = mockSensitiveFields();
    }

    @AfterAll
    public static void after() {
        httpClientPoolUtilMockedStatic.close();
    }

    @Test
    public void testEncryptByVault() throws Exception {
        String secretKey = RandomUtil.randomString(32);
        MockedStatic<KMSUtils> kmsUtilsMockedStatic = Mockito.mockStatic(KMSUtils.class);
        Mockito.when(KMSUtils.getVaultKey(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(new DataEncryptionKey(secretKey.getBytes(), 1));
        RuntimeContext runtimeContext = mockVaultRuntimeContext();
        Map<String, Object> map = new HashMap<>();
        map.put("identifier", CryptoProvider.Algorithm.AES_256_GCM96.getIdentifier());
        udfContext.setParameters(map);
        Encrypt encrypt = new Encrypt();
        encrypt.open(runtimeContext, udfContext);
        Event event = new Event();
        Map<String, Object> extractedFields = new HashMap<>();
        extractedFields.put("phone_number", DATA);
        event.setExtractedFields(extractedFields);
        Event result = encrypt.evaluate(event);
        Crypto crypto = CryptoProvider.createEncryptionAlgorithm(CryptoProvider.Algorithm.AES_256_GCM96.getIdentifier());
        assertNotNull(crypto);
        crypto.setDataEncryptionKey(new DataEncryptionKey(secretKey.getBytes(), 1));
        String encrypted = result.getExtractedFields().get("phone_number").toString();
        assertTrue(encrypted.contains("vault:v1:"));
        String decrypted = crypto.decrypt(encrypted.split(":")[2]);
        assertEquals(DATA, decrypted);
        encrypt.close();
        kmsUtilsMockedStatic.close();
    }

    @Test
    public void testEncryptByLocal() throws Exception {
        RuntimeContext runtimeContext = mockLocalRuntimeContext();
        Map<String, Object> map = new HashMap<>();
        map.put("identifier", CryptoProvider.Algorithm.AES_256_GCM96.getIdentifier());
        udfContext.setParameters(map);
        Encrypt encrypt = new Encrypt();
        encrypt.open(runtimeContext, udfContext);
        Event event = new Event();
        Map<String, Object> extractedFields = new HashMap<>();
        extractedFields.put("phone_number", DATA);
        event.setExtractedFields(extractedFields);
        Event result = encrypt.evaluate(event);
        Crypto encryptionAlgorithm = CryptoProvider.createEncryptionAlgorithm(CryptoProvider.Algorithm.AES_256_GCM96.getIdentifier());
        assertNotNull(encryptionAlgorithm);
        String decrypted = encryptionAlgorithm.decrypt((String) result.getExtractedFields().get("phone_number"));
        assertEquals(DATA, decrypted);
        encrypt.close();
    }

    @Test
    public void testEncryptGetKeyAndSensitiveFieldsError() throws Exception {
        String secretKey = RandomUtil.randomString(32);
        MockedStatic<KMSUtils> kmsUtilsMockedStatic = Mockito.mockStatic(KMSUtils.class);
        Mockito.when(KMSUtils.getVaultKey(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(new DataEncryptionKey(secretKey.getBytes(), 1));
        RuntimeContext runtimeContext = mockVaultRuntimeContext();
        Map<String, Object> map = new HashMap<>();
        map.put("identifier", CryptoProvider.Algorithm.AES_256_GCM96.getIdentifier());
        udfContext.setParameters(map);
        Encrypt encrypt = new Encrypt();
        encrypt.open(runtimeContext, udfContext);
        Event event = new Event();
        Map<String, Object> extractedFields = new HashMap<>();
        extractedFields.put("phone_number", DATA);
        event.setExtractedFields(extractedFields);
        Event result = encrypt.evaluate(event);
        Crypto crypto = CryptoProvider.createEncryptionAlgorithm(CryptoProvider.Algorithm.AES_256_GCM96.getIdentifier());
        assertNotNull(crypto);
        crypto.setDataEncryptionKey(new DataEncryptionKey(secretKey.getBytes(), 1));
        String encrypted = result.getExtractedFields().get("phone_number").toString();
        assertTrue(encrypted.contains("vault:v1:"));
        String decrypted = crypto.decrypt(encrypted.split(":")[2]);
        assertEquals(DATA, decrypted);

        Thread.sleep(90000);
        event = new Event();
        extractedFields = new HashMap<>();
        extractedFields.put("phone_number", DATA);
        event.setExtractedFields(extractedFields);
        result = encrypt.evaluate(event);
        encrypted = result.getExtractedFields().get("phone_number").toString();
        assertTrue(encrypted.contains("vault:v1:"));
        decrypted = crypto.decrypt(encrypted.split(":")[2]);
        assertEquals(DATA, decrypted);
        encrypt.close();
        kmsUtilsMockedStatic.close();
    }

    @Test
    public void testEncryptNoSensitiveFields() throws Exception {
        String secretKey = RandomUtil.randomString(32);
        MockedStatic<KMSUtils> kmsUtilsMockedStatic = Mockito.mockStatic(KMSUtils.class);
        Mockito.when(KMSUtils.getVaultKey(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(new DataEncryptionKey(secretKey.getBytes(), 1));
        RuntimeContext runtimeContext = mockVaultNoSensitiveFieldsRuntimeContext();
        Map<String, Object> map = new HashMap<>();
        map.put("identifier", CryptoProvider.Algorithm.AES_256_GCM96.getIdentifier());
        udfContext.setParameters(map);
        Encrypt encrypt = new Encrypt();
        encrypt.open(runtimeContext, udfContext);
        Event event = new Event();
        Map<String, Object> extractedFields = new HashMap<>();
        extractedFields.put("phone_number", DATA);
        event.setExtractedFields(extractedFields);
        Event result = encrypt.evaluate(event);
        Crypto crypto = CryptoProvider.createEncryptionAlgorithm(CryptoProvider.Algorithm.AES_256_GCM96.getIdentifier());
        assertNotNull(crypto);
        crypto.setDataEncryptionKey(new DataEncryptionKey(secretKey.getBytes(), 1));
        String encrypted = result.getExtractedFields().get("phone_number").toString();
        assertTrue(encrypted.contains("vault:v1:"));
        String decrypted = crypto.decrypt(encrypted.split(":")[2]);
        assertEquals(DATA, decrypted);
        encrypt.close();
        kmsUtilsMockedStatic.close();
    }

    @Test
    public void testEncryptFirstGetKeyError() throws Exception {
        RuntimeContext runtimeContext = mockVaultRuntimeContext();
        Map<String, Object> map = new HashMap<>();
        map.put("identifier", CryptoProvider.Algorithm.AES_256_GCM96.getIdentifier());
        udfContext.setParameters(map);
        Encrypt encrypt = new Encrypt();
        assertThrows(GrootStreamRuntimeException.class, () -> encrypt.open(runtimeContext, udfContext));
        encrypt.close();
    }

    @Test
    public void testEncryptFirstGetSensitiveFieldsError() throws Exception {
        httpClientPoolUtilMockedStatic.close();
        String secretKey = RandomUtil.randomString(32);
        MockedStatic<KMSUtils> kmsUtilsMockedStatic = Mockito.mockStatic(KMSUtils.class);
        Mockito.when(KMSUtils.getVaultKey(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(new DataEncryptionKey(secretKey.getBytes(), 1));
        RuntimeContext runtimeContext = mockVaultRuntimeContext();
        Map<String, Object> map = new HashMap<>();
        map.put("identifier", CryptoProvider.Algorithm.AES_256_GCM96.getIdentifier());
        udfContext.setParameters(map);
        Encrypt encrypt = new Encrypt();
        assertThrows(GrootStreamRuntimeException.class, () -> encrypt.open(runtimeContext, udfContext));
        encrypt.close();
        kmsUtilsMockedStatic.close();
        httpClientPoolUtilMockedStatic = mockSensitiveFields();
    }

    @Test
    public void testEncryptByVaultBySSL() throws Exception {
        ClientSSLConfig sslConfig = new ClientSSLConfig();
        sslConfig.setSkipVerification(true);
        assertDoesNotThrow(() -> KMSUtils.configureSSL(sslConfig));

        sslConfig.setSkipVerification(false);
        assertThrows(VaultException.class, () -> KMSUtils.configureSSL(sslConfig));

        sslConfig.setSkipVerification(false);
        sslConfig.setCaCertificatePath("src/test/resources/ssl/ca.crt");
        sslConfig.setCertificatePath("src/test/resources/ssl/server.crt");
        sslConfig.setPrivateKeyPath("src/test/resources/ssl/server.key");
        assertDoesNotThrow(() -> KMSUtils.configureSSL(sslConfig));
    }

    @Test
    public void testEncryptByIdentifier() {
        Map<String, Object> map = new HashMap<>();
        map.put("identifier", CryptoProvider.Algorithm.AES_256_GCM96.getIdentifier());
        udfContext.setParameters(map);
        Encrypt encrypt1 = new Encrypt();
        assertDoesNotThrow(() -> encrypt1.open(mockLocalRuntimeContext(), udfContext));
        encrypt1.close();

        Encrypt encrypt2 = new Encrypt();
        map.put("identifier", CryptoProvider.Algorithm.AES_128_GCM96.getIdentifier());
        udfContext.setParameters(map);
        assertDoesNotThrow(() -> encrypt2.open(mockLocalRuntimeContext(), udfContext));
        encrypt2.close();

        Encrypt encrypt3 = new Encrypt();
        map.put("identifier", CryptoProvider.Algorithm.SM4_GCM96.getIdentifier());
        udfContext.setParameters(map);
        assertDoesNotThrow(() -> encrypt3.open(mockLocalRuntimeContext(), udfContext));
        encrypt3.close();

        Encrypt encrypt4 = new Encrypt();
        map.put("identifier", CryptoProvider.Algorithm.AES_128_GCM.getIdentifier());
        udfContext.setParameters(map);
        assertDoesNotThrow(() -> encrypt4.open(mockLocalRuntimeContext(), udfContext));
        encrypt4.close();
    }

    @Test
    public void testEncryptionAlgorithm() throws Exception {
        Crypto crypto = CryptoProvider.createEncryptionAlgorithm(CryptoProvider.Algorithm.AES_128_GCM96.getIdentifier());
        assertNotNull(crypto);
        crypto.setDataEncryptionKey(new DataEncryptionKey("aaaaaaaaaaaaaaaa".getBytes(), 1));
        String encryptData = crypto.encrypt(DATA);
        String decryptData = crypto.decrypt(encryptData);
        assertEquals(DATA, decryptData);

        crypto = CryptoProvider.createEncryptionAlgorithm(CryptoProvider.Algorithm.AES_256_GCM96.getIdentifier());
        assertNotNull(crypto);
        crypto.setDataEncryptionKey(new DataEncryptionKey("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".getBytes(), 1));
        encryptData = crypto.encrypt(DATA);
        decryptData = crypto.decrypt(encryptData);
        assertEquals(DATA, decryptData);

        crypto = CryptoProvider.createEncryptionAlgorithm(CryptoProvider.Algorithm.SM4_GCM96.getIdentifier());
        assertNotNull(crypto);
        crypto.setDataEncryptionKey(new DataEncryptionKey("aaaaaaaaaaaaaaaa".getBytes(), 1));
        encryptData = crypto.encrypt(DATA);
        decryptData = crypto.decrypt(encryptData);
        assertEquals(DATA, decryptData);

        crypto = CryptoProvider.createEncryptionAlgorithm(CryptoProvider.Algorithm.AES_128_GCM.getIdentifier());
        assertNotNull(crypto);
        crypto.setDataEncryptionKey(new DataEncryptionKey("aaaaaaaaaaaaaaaa".getBytes(), 1));
        encryptData = crypto.encrypt(DATA);
        assertEquals("R2FsYXh5MjAxOSMq6Q4PFGRvBmtSQ36Ug9XDHyMXB7Oye/OPITNW", encryptData);
        decryptData = crypto.decrypt(encryptData);
        assertEquals(DATA, decryptData);

        assertThrows(IllegalArgumentException.class, () -> CryptoProvider.createEncryptionAlgorithm("sm4"));
    }

    @Test
    public void testEncryptError() {
        RuntimeContext runtimeContext = mockLocalRuntimeContext();
        Encrypt encrypt = new Encrypt();
        udfContext.setParameters(null);
        assertThrows(GrootStreamRuntimeException.class, () -> encrypt.open(runtimeContext, udfContext));

        Map<String, Object> map = new HashMap<>();
        udfContext.setParameters(map);
        assertThrows(GrootStreamRuntimeException.class, () -> encrypt.open(runtimeContext, udfContext));

        map.put("identifier", "aes");
        udfContext.setParameters(map);
        assertThrows(GrootStreamRuntimeException.class, () -> encrypt.open(runtimeContext, udfContext));
    }

    static RuntimeContext mockLocalRuntimeContext() {
        RuntimeContext runtimeContext = Mockito.mock(RuntimeContext.class);
        ExecutionConfig executionConfig = Mockito.mock(ExecutionConfig.class);
        Mockito.when(runtimeContext.getExecutionConfig()).thenReturn(executionConfig);
        MetricGroup metricGroup = Mockito.mock(OperatorMetricGroup.class);
        Mockito.when(runtimeContext.getMetricGroup()).thenReturn(metricGroup);
        Mockito.when(metricGroup.addGroup(Mockito.anyString())).thenReturn(metricGroup);
        Mockito.when(metricGroup.counter(Mockito.anyString())).thenReturn(new SimpleCounter());
        Configuration configuration = new Configuration();
        CommonConfig commonConfig = new CommonConfig();
        Map<String, KmsConfig> kmsConfigs = new HashMap<>();
        KmsConfig kmsConfig = new KmsConfig();
        kmsConfig.setType(KMSUtils.KMS_TYPE_LOCAL);
        kmsConfigs.put(KMSUtils.KMS_TYPE_LOCAL, kmsConfig);
        kmsConfig = new KmsConfig();
        kmsConfig.setType(KMSUtils.KMS_TYPE_VAULT);
        kmsConfigs.put(KMSUtils.KMS_TYPE_VAULT, kmsConfig);
        ClientSSLConfig sslConfig = new ClientSSLConfig();
        sslConfig.setSkipVerification(true);
        Map<String, String> propertiesConfig = new HashMap<>();
        propertiesConfig.put("projection.encrypt.schema.registry.uri", "127.0.0.1:9999/v1/schema/session_record?option=encrypt_fields");
        commonConfig.setKmsConfig(kmsConfigs);
        commonConfig.setSslConfig(sslConfig);
        commonConfig.setPropertiesConfig(propertiesConfig);
        configuration.setString(Constants.SYSPROP_GROOTSTREAM_CONFIG, JSON.toJSONString(commonConfig));
        configuration.setString(Constants.SYSPROP_KMS_TYPE_CONFIG, KMSUtils.KMS_TYPE_LOCAL);
        Mockito.when(executionConfig.getGlobalJobParameters()).thenReturn(configuration);
        return runtimeContext;
    }

    static RuntimeContext mockVaultRuntimeContext() {
        RuntimeContext runtimeContext = Mockito.mock(RuntimeContext.class);
        ExecutionConfig executionConfig = Mockito.mock(ExecutionConfig.class);
        Mockito.when(runtimeContext.getExecutionConfig()).thenReturn(executionConfig);
        MetricGroup metricGroup = Mockito.mock(OperatorMetricGroup.class);
        Mockito.when(runtimeContext.getMetricGroup()).thenReturn(metricGroup);
        Mockito.when(metricGroup.addGroup(Mockito.anyString())).thenReturn(metricGroup);
        Mockito.when(metricGroup.counter(Mockito.anyString())).thenReturn(new SimpleCounter());
        Configuration configuration = new Configuration();
        CommonConfig commonConfig = new CommonConfig();
        Map<String, KmsConfig> kmsConfigs = new HashMap<>();
        KmsConfig kmsConfig = new KmsConfig();
        kmsConfig.setType(KMSUtils.KMS_TYPE_VAULT);
        kmsConfigs.put(KMSUtils.KMS_TYPE_VAULT, kmsConfig);
        kmsConfig = new KmsConfig();
        kmsConfig.setType(KMSUtils.KMS_TYPE_LOCAL);
        kmsConfigs.put(KMSUtils.KMS_TYPE_LOCAL, kmsConfig);
        ClientSSLConfig sslConfig = new ClientSSLConfig();
        sslConfig.setSkipVerification(true);
        Map<String, String> propertiesConfig = new HashMap<>();
        propertiesConfig.put("projection.encrypt.schema.registry.uri", "127.0.0.1:9999/v1/schema/session_record?option=encrypt_fields");
        propertiesConfig.put(Constants.SYSPROP_ENCRYPT_KMS_KEY_SCHEDULER_INTERVAL_NAME, "1");
        propertiesConfig.put(Constants.SYSPROP_ENCRYPT_SENSITIVE_FIELDS_SCHEDULER_INTERVAL_NAME, "1");
        commonConfig.setKmsConfig(kmsConfigs);
        commonConfig.setSslConfig(sslConfig);
        commonConfig.setPropertiesConfig(propertiesConfig);
        configuration.setString(Constants.SYSPROP_GROOTSTREAM_CONFIG, JSON.toJSONString(commonConfig));
        configuration.setString(Constants.SYSPROP_KMS_TYPE_CONFIG, KMSUtils.KMS_TYPE_VAULT);
        Mockito.when(executionConfig.getGlobalJobParameters()).thenReturn(configuration);
        return runtimeContext;
    }

    static RuntimeContext mockVaultNoSensitiveFieldsRuntimeContext() {
        RuntimeContext runtimeContext = Mockito.mock(RuntimeContext.class);
        ExecutionConfig executionConfig = Mockito.mock(ExecutionConfig.class);
        Mockito.when(runtimeContext.getExecutionConfig()).thenReturn(executionConfig);
        MetricGroup metricGroup = Mockito.mock(OperatorMetricGroup.class);
        Mockito.when(runtimeContext.getMetricGroup()).thenReturn(metricGroup);
        Mockito.when(metricGroup.addGroup(Mockito.anyString())).thenReturn(metricGroup);
        Mockito.when(metricGroup.counter(Mockito.anyString())).thenReturn(new SimpleCounter());
        Configuration configuration = new Configuration();
        CommonConfig commonConfig = new CommonConfig();
        Map<String, KmsConfig> kmsConfigs = new HashMap<>();
        KmsConfig kmsConfig = new KmsConfig();
        kmsConfig.setType(KMSUtils.KMS_TYPE_VAULT);
        kmsConfigs.put(KMSUtils.KMS_TYPE_VAULT, kmsConfig);
        kmsConfig = new KmsConfig();
        kmsConfig.setType(KMSUtils.KMS_TYPE_LOCAL);
        kmsConfigs.put(KMSUtils.KMS_TYPE_LOCAL, kmsConfig);
        ClientSSLConfig sslConfig = new ClientSSLConfig();
        sslConfig.setSkipVerification(true);
        Map<String, String> propertiesConfig = new HashMap<>();
        propertiesConfig.put(Constants.SYSPROP_ENCRYPT_KMS_KEY_SCHEDULER_INTERVAL_NAME, "1");
        propertiesConfig.put(Constants.SYSPROP_ENCRYPT_SENSITIVE_FIELDS_SCHEDULER_INTERVAL_NAME, "1");
        commonConfig.setKmsConfig(kmsConfigs);
        commonConfig.setSslConfig(sslConfig);
        commonConfig.setPropertiesConfig(propertiesConfig);
        configuration.setString(Constants.SYSPROP_GROOTSTREAM_CONFIG, JSON.toJSONString(commonConfig));
        configuration.setString(Constants.SYSPROP_KMS_TYPE_CONFIG, KMSUtils.KMS_TYPE_VAULT);
        Mockito.when(executionConfig.getGlobalJobParameters()).thenReturn(configuration);
        return runtimeContext;
    }

    static MockedStatic<HttpClientPoolUtil> mockSensitiveFields() throws IOException {
        String sensitiveFieldsStr = "{\n" +
                "    \"status\": 200,\n" +
                "    \"success\": true,\n" +
                "    \"message\": \"Success\",\n" +
                "    \"data\": [\n" +
                "        \"phone_number\",\n" +
                "        \"server_ip\"\n" +
                "    ]\n" +
                "}";
        HttpClientPoolUtil instance = Mockito.mock(HttpClientPoolUtil.class);
        Mockito.when(instance.httpGet(ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(sensitiveFieldsStr);
        MockedStatic<HttpClientPoolUtil> httpClientPoolUtilMockedStatic = Mockito.mockStatic(HttpClientPoolUtil.class);
        Mockito.when(HttpClientPoolUtil.getInstance()).thenReturn(instance);
        return httpClientPoolUtilMockedStatic;
    }
}
