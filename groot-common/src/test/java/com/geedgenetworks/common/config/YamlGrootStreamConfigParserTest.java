package com.geedgenetworks.common.config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class YamlGrootStreamConfigParserTest {

    @Test
    public void testGrootStreamConfig() {
        CommonConfigLocator commonConfigLocator = new CommonConfigLocator();
        GrootStreamConfig config;
        if (commonConfigLocator.locateInWorkDirOrOnClasspath()) {
            // 2. Try loading YAML config from the working directory or from the classpath
            config = new GrootStreamConfigBuilder(commonConfigLocator).build();
        } else {
            throw new RuntimeException("can't find yaml in resources");
        }
        Assertions.assertNotNull(config);
        Assertions.assertNotNull(config.getCommonConfig().getKnowledgeBaseConfig());
        Assertions.assertTrue(config.getCommonConfig().getKnowledgeBaseConfig().size() > 0);
        Assertions.assertTrue(
                Arrays.asList("http", "hdfs", "local")
                        .contains(
                                config.getCommonConfig()
                                        .getKnowledgeBaseConfig()
                                        .get(0)
                                        .getFsType()));
        Assertions.assertTrue(config.getUDFPluginConfig().size() > 0);
    }
}
