package com.geedgenetworks.bootstrap.utils;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;

@Slf4j
public final class EnvironmentUtil {
    private  EnvironmentUtil() {
        throw new UnsupportedOperationException("EnvironmentUtil is a utility class and cannot be instantiated");
    }

    public static void initConfiguration(Config appConfig, Configuration configuration) {
        //todo
    }


}
