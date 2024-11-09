package com.geedgenetworks.bootstrap.utils;

import cn.hutool.setting.yaml.YamlUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.geedgenetworks.common.config.Constants;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValueFactory;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Map;

@Slf4j
public class CryptoShadeTest {

    private static final String USERNAME = "grootstream";

    private static final String PASSWORD = "grootstream_password";

    @Test
    public void testParseConfig() throws URISyntaxException {
        URL resource = CryptoShadeTest.class.getResource("/inline_to_clickhouse.yaml");
        Assertions.assertNotNull(resource);
        Map<String, Object> configMap = YamlUtil.loadByPath(Paths.get(resource.toURI()).toString());
        ConfigObject configObject = ConfigValueFactory.fromMap(configMap);
        Config decryptConfig = CryptoShadeUtils.decryptConfig(configObject.toConfig());
        String jsonString = decryptConfig.root().render(ConfigRenderOptions.concise());
        JSONObject jsonObject = JSON.parseObject(jsonString);
        JSONObject sinkObject = jsonObject.getJSONObject(Constants.SINKS);
        log.info("Decrypt config: {}", decryptConfig.root().render(ConfigRenderOptions.concise()));
        Assertions.assertEquals(sinkObject.keySet().size(), 1);
        Assertions.assertNotNull(sinkObject);
        Assertions.assertNotNull(sinkObject.getJSONObject("clickhouse_sink"));
        Assertions.assertNotNull(sinkObject.getJSONObject("clickhouse_sink")
                .getJSONObject("properties"));
        Assertions.assertEquals(sinkObject.getJSONObject("clickhouse_sink")
                .getJSONObject("properties").isEmpty(), false);
        Assertions.assertEquals(sinkObject.getJSONObject("clickhouse_sink")
                .getJSONObject("properties").get("connection.user"), USERNAME);
        Assertions.assertNotNull(sinkObject.getJSONObject("clickhouse_sink")
                .getJSONObject("properties").get("connection.password"), PASSWORD);
    }

    @Test
    public void testDecryptAndEncrypt() {
        String encryptUsername = CryptoShadeUtils.encryptOption("base64", USERNAME);
        String decryptUsername = CryptoShadeUtils.decryptOption("base64", encryptUsername);
        String encryptPassword = CryptoShadeUtils.encryptOption("base64", PASSWORD);
        String decryptPassword = CryptoShadeUtils.decryptOption("base64", encryptPassword);
        Assertions.assertEquals("Z3Jvb3RzdHJlYW0=", encryptUsername);
        Assertions.assertEquals("Z3Jvb3RzdHJlYW1fcGFzc3dvcmQ=", encryptPassword);
        Assertions.assertEquals(decryptUsername, USERNAME);
        Assertions.assertEquals(decryptPassword, PASSWORD);

        encryptUsername = CryptoShadeUtils.encryptOption("aes", USERNAME);
        decryptUsername = CryptoShadeUtils.decryptOption("aes", encryptUsername);
        encryptPassword = CryptoShadeUtils.encryptOption("aes", PASSWORD);
        decryptPassword = CryptoShadeUtils.decryptOption("aes", encryptPassword);
        Assertions.assertEquals("7ZhjN9/b40G+HSlwLmrmGQ==", encryptUsername);
        Assertions.assertEquals("FZx9qD2Yip7AQdEKa/viIby67Wti2cwbBP9R5jPr0QU=", encryptPassword);
        Assertions.assertEquals(decryptUsername, USERNAME);
        Assertions.assertEquals(decryptPassword, PASSWORD);

        encryptUsername = CryptoShadeUtils.encryptOption("sm4", USERNAME);
        decryptUsername = CryptoShadeUtils.decryptOption("sm4", encryptUsername);
        Assertions.assertEquals("cup0NnoVy5aw0dQhBBSVGQ==", encryptUsername);
        Assertions.assertEquals(decryptUsername, USERNAME);
        encryptPassword = CryptoShadeUtils.encryptOption("sm4", PASSWORD);
        decryptPassword = CryptoShadeUtils.decryptOption("sm4", encryptPassword);
        Assertions.assertEquals("OHbHCI05W7v6gm42SLbJoCLn+AlBwTIxO95tyKfyNR8=", encryptPassword);
        Assertions.assertEquals(decryptPassword, PASSWORD);

        System.out.println(CryptoShadeUtils.encryptOption("sm4", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"galaxy2019\";"));
        System.out.println(CryptoShadeUtils.decryptOption("sm4", "f76480be84a8ee1b009504c6c56a5bed48239c348a468f94b4029a6a3148f51530b025d6dfa140af93b4c7c6fe0e3dce543773e779d272b5579555fbd3271e7fdbee088673a901b3f3b28e914a25f30a4a859d97594c5ea7d7c1dcefe8c62560baea32b6da0b767232ed8aca17af2dc6"));
        System.out.println(CryptoShadeUtils.encryptOption("aes", "testuser"));
        System.out.println(CryptoShadeUtils.encryptOption("aes", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"olap\" password=\"galaxy2019\";"));

        encryptUsername = CryptoShadeUtils.encryptOption("sm4-gcm96", USERNAME);
        decryptUsername = CryptoShadeUtils.decryptOption("sm4-gcm96", encryptUsername);
        encryptPassword = CryptoShadeUtils.encryptOption("sm4-gcm96", PASSWORD);
        decryptPassword = CryptoShadeUtils.decryptOption("sm4-gcm96", encryptPassword);
        Assertions.assertEquals(decryptUsername, USERNAME);
        Assertions.assertEquals(decryptPassword, PASSWORD);

        encryptUsername = CryptoShadeUtils.encryptOption("aes-128-gcm96", USERNAME);
        decryptUsername = CryptoShadeUtils.decryptOption("aes-128-gcm96", encryptUsername);
        encryptPassword = CryptoShadeUtils.encryptOption("aes-128-gcm96", PASSWORD);
        decryptPassword = CryptoShadeUtils.decryptOption("aes-128-gcm96", encryptPassword);
        Assertions.assertEquals(decryptUsername, USERNAME);
        Assertions.assertEquals(decryptPassword, PASSWORD);

        encryptUsername = CryptoShadeUtils.encryptOption("aes-256-gcm96", USERNAME);
        decryptUsername = CryptoShadeUtils.decryptOption("aes-256-gcm96", encryptUsername);
        encryptPassword = CryptoShadeUtils.encryptOption("aes-256-gcm96", PASSWORD);
        decryptPassword = CryptoShadeUtils.decryptOption("aes-256-gcm96", encryptPassword);
        Assertions.assertEquals(decryptUsername, USERNAME);
        Assertions.assertEquals(decryptPassword, PASSWORD);
    }
}
