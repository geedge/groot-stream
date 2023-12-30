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

    CheckResult checkConfig();
    RuntimeEnvironment prepare();

    void registerPlugin(List<URL> pluginPaths);
    default void initialize(Config config) {
        this.setEnvConfig(config.getConfig(ConfigUtil.joinPath(Constants.APPLICATION, Constants.APPLICATION_ENV))).prepare();
    }
}
