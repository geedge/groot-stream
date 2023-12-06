package com.geedgenetworks.common.config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class YamlGrootStreamConfigParserTest {

    @Test
    public void testGrootStreamConfig() {
        YamlGrootStreamConfigLocator yamlConfigLocator = new YamlGrootStreamConfigLocator();
        GrootStreamConfig config;
        if (yamlConfigLocator.locateInWorkDirOrOnClasspath()) {
            // 2. Try loading YAML config from the working directory or from the classpath
            config = new YamlGrootStreamConfigBuilder(yamlConfigLocator).setProperties(null).build();
        } else {
            throw new RuntimeException("can't find yaml in resources");
        }
        Assertions.assertNotNull(config);
        Assertions.assertEquals(90, config.getEngineConfig().getTickTupleFreqSecs());
        Assertions.assertNotNull(config.getEngineConfig().getNacosConfig().getServerAddr());
        Assertions.assertTrue(config.getEngineConfig().getHttpConPoolConfig().getSocketTimeout() > 0);
        Assertions.assertNotNull(config.getEngineConfig().getKnowledgeBaseConfig());
        Assertions.assertEquals("hos", config.getEngineConfig().getKnowledgeBaseConfig().get(0).getProperties().get("fs_type"));

    }

}
