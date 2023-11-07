package com.geedgenetworks.bootstrap.execution;


import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.config.CheckResult;
import com.typesafe.config.Config;

import java.net.URL;
import java.util.List;

public interface RuntimeEnvironment {

    //storage application config
    RuntimeEnvironment setConfig(Config config);

    Config getConfig();

    CheckResult checkConfig();

    RuntimeEnvironment prepare();

    void registerPlugin(List<URL> pluginPaths);

    default void initialize(Config config) {
        this.setConfig(config.getConfig(Constants.APPLICATION)).prepare();
    }





}
