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
        encryptUsername = ConfigShadeUtils.encryptOption("sm4", USERNAME);
        decryptUsername = ConfigShadeUtils.decryptOption("sm4", encryptUsername);
        Assertions.assertEquals("72ea74367a15cb96b0d1d42104149519", encryptUsername);
        Assertions.assertEquals(decryptUsername, USERNAME);
        encryptPassword = ConfigShadeUtils.encryptOption("sm4", PASSWORD);
        decryptPassword = ConfigShadeUtils.decryptOption("sm4", encryptPassword);
        Assertions.assertEquals("3876c7088d395bbbfa826e3648b6c9a022e7f80941c132313bde6dc8a7f2351f", encryptPassword);
        Assertions.assertEquals(decryptPassword, PASSWORD);
        System.out.println( ConfigShadeUtils.encryptOption("sm4", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\"galaxy2019\";"));
        System.out.println( ConfigShadeUtils.decryptOption("sm4", "f76480be84a8ee1b009504c6c56a5bed48239c348a468f94b4029a6a3148f51530b025d6dfa140af93b4c7c6fe0e3dce543773e779d272b5579555fbd3271e7fdbee088673a901b3f3b28e914a25f30a4a859d97594c5ea7d7c1dcefe8c62560baea32b6da0b767232ed8aca17af2dc6"));
        System.out.println( ConfigShadeUtils.encryptOption("aes", "testuser"));
        System.out.println( ConfigShadeUtils.encryptOption("aes", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"olap\" password=\"galaxy2019\";"));
    }
}
