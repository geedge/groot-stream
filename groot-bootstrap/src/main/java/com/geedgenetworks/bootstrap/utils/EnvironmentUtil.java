package com.geedgenetworks.bootstrap.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;

import java.util.Map;

@Slf4j
public final class EnvironmentUtil {
    private  EnvironmentUtil() {
        throw new UnsupportedOperationException("EnvironmentUtil is a utility class and cannot be instantiated");
    }

    public static void initConfiguration(Map<String, Object> appConfig, Configuration configuration) {
        //todo
    }


}
