package com.geedgenetworks.bootstrap.execution;


import com.geedgenetworks.common.config.CheckResult;

import java.net.URL;
import java.util.List;
import java.util.Map;

public interface RuntimeEnvironment {

    //storage application config
    RuntimeEnvironment setConfig(Map<String, Object> appConfig);

    Map<String, Object> getConfig();

    CheckResult checkConfig();

    RuntimeEnvironment prepare();

    void registerPlugin(List<URL> pluginPaths);

    default void initialize(Map<String, Object> appConfig) {
        this.setConfig(appConfig).prepare();
    }





}
