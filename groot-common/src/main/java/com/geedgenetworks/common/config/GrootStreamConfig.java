package com.geedgenetworks.common.config;

import com.geedgenetworks.common.Constants;
import com.hazelcast.config.Config;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.List;

@Slf4j
public class GrootStreamConfig {
    private final EngineConfig engineConfig = new EngineConfig();

    private Config hazelcastConfig;

    private List<String> udfMappingConfig;

    static {
        String value = grootStreamHome();
        log.info("groot-stream.home is " + value);
        System.setProperty(GrootStreamProperties.GROOTSTREAM_HOME.getName(), value);
    }

    public GrootStreamConfig() {
        hazelcastConfig = new Config();
        hazelcastConfig
                .getNetworkConfig()
                .getJoin()
                .getMulticastConfig()
                .setMulticastPort(Constants.DEFAULT_GROOTSTREAM_MULTICAST_PORT);
        hazelcastConfig
                .getHotRestartPersistenceConfig()
                .setBaseDir(new File(grootStreamHome(), "recovery").getAbsoluteFile());
    }



    /**
     * Returns the absolute path for `groot-stream.home` based from the system property {@link
     * GrootStreamProperties#GROOTSTREAM_HOME}
     */
    private static String grootStreamHome() {
        return new File(
                System.getProperty(
                        GrootStreamProperties.GROOTSTREAM_HOME.getName(),
                        GrootStreamProperties.GROOTSTREAM_HOME.getDefaultValue()))
                .getAbsolutePath();
    }
    public Config getHazelcastConfig() {
        return hazelcastConfig;
    }

    public void setHazelcastConfig(Config hazelcastConfig) {
        this.hazelcastConfig = hazelcastConfig;
    }
    public EngineConfig getEngineConfig() {
        return engineConfig;
    }


    public List<String> getUdfMappingConfig() {
        return udfMappingConfig;
    }

    public void setUdfMappingConfig(List<String> udfMappingConfig) {
        this.udfMappingConfig = udfMappingConfig;
    }
}
