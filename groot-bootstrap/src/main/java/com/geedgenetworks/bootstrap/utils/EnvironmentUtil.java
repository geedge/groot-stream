package com.geedgenetworks.bootstrap.utils;

import com.geedgenetworks.bootstrap.execution.ExecutionConfigKey;
import com.geedgenetworks.common.config.CheckResult;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;

import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
public final class EnvironmentUtil {
    private  EnvironmentUtil() {
        throw new UnsupportedOperationException("EnvironmentUtil is a utility class and cannot be instantiated");
    }

    public static void initConfiguration(Config appConfig, Configuration configuration) {
        if (appConfig.hasPath("pipeline")) {
            Config pipeline = appConfig.getConfig("pipeline");
            if (pipeline.hasPath("jars")) {
                configuration.setString(PipelineOptions.JARS.key(), pipeline.getString("jars"));
            }
            if (pipeline.hasPath("classpaths")) {
                configuration.setString(
                        PipelineOptions.CLASSPATHS.key(), pipeline.getString("classpaths"));
            }
        }
        String prefixConf = "flink.";
        if (!appConfig.isEmpty()) {
            for (Map.Entry<String, ConfigValue> entryConfKey : appConfig.entrySet()) {
                String confKey = entryConfKey.getKey().trim();

                if (confKey.startsWith(prefixConf)) {
                    configuration.setString(
                            confKey.replaceFirst(prefixConf, ""), entryConfKey.getValue().render());
                }
            }
        }
    }

    public static void setRestartStrategy(Config config, ExecutionConfig executionConfig) {
        try {
            if (config.hasPath(ExecutionConfigKey.RESTART_STRATEGY)) {
                String restartStrategy = config.getString(ExecutionConfigKey.RESTART_STRATEGY);
                switch (restartStrategy.toLowerCase()) {
                    case "no":
                        executionConfig.setRestartStrategy(RestartStrategies.noRestart());
                        break;
                    case "fixed-delay":
                        int attempts = config.getInt(ExecutionConfigKey.RESTART_ATTEMPTS);
                        long delay = config.getLong(ExecutionConfigKey.RESTART_DELAY_BETWEEN_ATTEMPTS);
                        executionConfig.setRestartStrategy(
                                RestartStrategies.fixedDelayRestart(attempts, delay));
                        break;
                    case "failure-rate":
                        long failureInterval =
                                config.getLong(ExecutionConfigKey.RESTART_FAILURE_INTERVAL);
                        int rate = config.getInt(ExecutionConfigKey.RESTART_FAILURE_RATE);
                        long delayInterval = config.getLong(ExecutionConfigKey.RESTART_DELAY_INTERVAL);
                        executionConfig.setRestartStrategy(
                                RestartStrategies.failureRateRestart(
                                        rate,
                                        Time.of(failureInterval, TimeUnit.MILLISECONDS),
                                        Time.of(delayInterval, TimeUnit.MILLISECONDS)));
                        break;
                    default:
                        log.warn(
                                "set restart.strategy failed, unknown restart.strategy [{}],only support no,fixed-delay,failure-rate",
                                restartStrategy);
                }
            }
        } catch (Exception e) {
            log.warn("set restart.strategy in config '{}' exception", config, e);
        }
    }

    public static CheckResult checkRestartStrategy(Config config) {
        if (config.hasPath(ExecutionConfigKey.RESTART_STRATEGY)) {
            String restartStrategy = config.getString(ExecutionConfigKey.RESTART_STRATEGY);
            switch (restartStrategy.toLowerCase()) {
                case "fixed-delay":
                    if (!(config.hasPath(ExecutionConfigKey.RESTART_ATTEMPTS)
                            && config.hasPath(ExecutionConfigKey.RESTART_DELAY_BETWEEN_ATTEMPTS))) {
                        return CheckResult.error(
                                String.format(
                                        "fixed-delay restart strategy must set [%s],[%s]",
                                        ExecutionConfigKey.RESTART_ATTEMPTS,
                                        ExecutionConfigKey.RESTART_DELAY_BETWEEN_ATTEMPTS));
                    }
                    break;
                case "failure-rate":
                    if (!(config.hasPath(ExecutionConfigKey.RESTART_FAILURE_INTERVAL)
                            && config.hasPath(ExecutionConfigKey.RESTART_FAILURE_RATE)
                            && config.hasPath(ExecutionConfigKey.RESTART_DELAY_INTERVAL))) {
                        return CheckResult.error(
                                String.format(
                                        "failure-rate restart strategy must set [%s],[%s],[%s]",
                                        ExecutionConfigKey.RESTART_FAILURE_INTERVAL,
                                        ExecutionConfigKey.RESTART_FAILURE_RATE,
                                        ExecutionConfigKey.RESTART_DELAY_INTERVAL));
                    }
                    break;
                default:
                    return CheckResult.success();
            }
        }
        return CheckResult.success();
    }



}
