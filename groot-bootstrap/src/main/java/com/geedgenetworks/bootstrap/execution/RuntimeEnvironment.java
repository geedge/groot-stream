package com.geedgenetworks.bootstrap.execution;


import com.geedgenetworks.bootstrap.enums.TargetType;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.config.CheckResult;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigUtil;

import java.net.URL;
import java.util.List;

public interface RuntimeEnvironment {
    RuntimeEnvironment setEnvConfig(Config envConfig);
    Config getEnvConfig();
    RuntimeEnvironment loadJobProperties(Config jobProperties);
    CheckResult checkConfig();
    RuntimeEnvironment prepare();

    void registerPlugin(List<URL> pluginPaths);
    default void initialize(Config config) {
        if (config.getConfig(Constants.APPLICATION).hasPath(Constants.PROPERTIES)) {
            this.loadJobProperties(config.getConfig(ConfigUtil.joinPath(Constants.APPLICATION, Constants.PROPERTIES)));
        }
        this.setEnvConfig(config.getConfig(ConfigUtil.joinPath(Constants.APPLICATION, Constants.APPLICATION_ENV))).prepare();
    }
}
