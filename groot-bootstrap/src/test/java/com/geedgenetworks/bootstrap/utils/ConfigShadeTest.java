package com.geedgenetworks.bootstrap.utils;

import cn.hutool.setting.yaml.YamlUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.geedgenetworks.common.Constants;
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
public class ConfigShadeTest {

    private static final String USERNAME = "grootstream";

    private static final String PASSWORD = "grootstream_password";

    @Test
    public void testParseConfig() throws URISyntaxException {
        URL resource = ConfigShadeTest.class.getResource("/inline_to_clickhouse.yaml");
        Assertions.assertNotNull(resource);
        Map<String, Object> configMap = YamlUtil.loadByPath(Paths.get(resource.toURI()).toString());
        ConfigObject configObject = ConfigValueFactory.fromMap(configMap);
        Config decryptConfig = ConfigShadeUtils.decryptConfig(configObject.toConfig());
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
                                                        .getJSONObject("properties").get("connection.user"),USERNAME);
        Assertions.assertNotNull(sinkObject.getJSONObject("clickhouse_sink")
                                                        .getJSONObject("properties").get("connection.password"), PASSWORD);
    }

    @Test
    public void testDecryptAndEncrypt() {
        String encryptUsername = ConfigShadeUtils.encryptOption("base64", USERNAME);
        String decryptUsername = ConfigShadeUtils.decryptOption("base64", encryptUsername);
        String encryptPassword = ConfigShadeUtils.encryptOption("base64", PASSWORD);
        String decryptPassword = ConfigShadeUtils.decryptOption("base64", encryptPassword);
        Assertions.assertEquals("Z3Jvb3RzdHJlYW0=", encryptUsername);
        Assertions.assertEquals("Z3Jvb3RzdHJlYW1fcGFzc3dvcmQ=", encryptPassword);
        Assertions.assertEquals(decryptUsername, USERNAME);
        Assertions.assertEquals(decryptPassword, PASSWORD);
        encryptUsername = ConfigShadeUtils.encryptOption("aes", USERNAME);
        decryptUsername = ConfigShadeUtils.decryptOption("aes", encryptUsername);
        encryptPassword = ConfigShadeUtils.encryptOption("aes", PASSWORD);
        decryptPassword = ConfigShadeUtils.decryptOption("aes", encryptPassword);
        Assertions.assertEquals("ed986337dfdbe341be1d29702e6ae619", encryptUsername);
        Assertions.assertEquals("159c7da83d988a9ec041d10a6bfbe221bcbaed6b62d9cc1b04ff51e633ebd105", encryptPassword);
        Assertions.assertEquals(decryptUsername, USERNAME);
        Assertions.assertEquals(decryptPassword, PASSWORD);
        System.out.println( ConfigShadeUtils.encryptOption("aes", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"galaxy2019\";"));
        System.out.println( ConfigShadeUtils.decryptOption("aes", "454f65ea6eef1256e3067104f82730e737b68959560966b811e7ff364116b03124917eb2b0f3596f14733aa29ebad9352644ce1a5c85991c6f01ba8a5e8f177a7ff0b2d3889a424249967b3870b50993d9644f239f0de82cdb13bdb502959e16afadffa49ef1e1d2b9c9b5113e619817"));
        System.out.println( ConfigShadeUtils.encryptOption("aes", "testuser"));
    }
}
