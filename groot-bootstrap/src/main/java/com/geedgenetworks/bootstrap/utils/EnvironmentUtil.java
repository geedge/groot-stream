package com.geedgenetworks.bootstrap.utils;

import com.geedgenetworks.bootstrap.execution.ExecutionConfigKeyName;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.config.CheckResult;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigUtil;
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
    private EnvironmentUtil() {
        throw new UnsupportedOperationException("EnvironmentUtil is a utility class and cannot be instantiated");
    }

    public static void initConfiguration(Config envConfig, Configuration configuration) {
        if (envConfig.hasPath("pipeline")) {
            Config pipeline = envConfig.getConfig("pipeline");
            if (pipeline.hasPath("jars")) {
                configuration.setString(PipelineOptions.JARS.key(), pipeline.getString("jars"));
            }
            if (pipeline.hasPath("classpaths")) {
                configuration.setString(
                        PipelineOptions.CLASSPATHS.key(), pipeline.getString("classpaths"));
            }
            if (pipeline.hasPath("object-reuse")) {
                configuration.setBoolean(PipelineOptions.OBJECT_REUSE.key(), pipeline.getBoolean("object-reuse"));
            }
        }
        if (envConfig.hasPath(ConfigUtil.joinPath(Constants.SYSPROP_KMS_TYPE_CONFIG))) {
            configuration.setString(Constants.SYSPROP_KMS_TYPE_CONFIG, envConfig.getString(ConfigUtil.joinPath(Constants.SYSPROP_KMS_TYPE_CONFIG)));
        }
        String prefixConf = "flink.";
        if (!envConfig.isEmpty()) {
            for (Map.Entry<String, ConfigValue> entryConfKey : envConfig.entrySet()) {
                String confKey = entryConfKey.getKey().trim();

                if (confKey.startsWith(prefixConf)) {
                    configuration.setString(
                            confKey.replaceFirst(prefixConf, ""), entryConfKey.getValue().unwrapped().toString());
                }
            }
        }
    }

    public static void setRestartStrategy(Config envConfig, ExecutionConfig executionConfig) {
        try {
            if (envConfig.hasPath(ExecutionConfigKeyName.RESTART_STRATEGY)) {
                String restartStrategy = envConfig.getString(ExecutionConfigKeyName.RESTART_STRATEGY);
                switch (restartStrategy.toLowerCase()) {
                    case "none":
                        executionConfig.setRestartStrategy(RestartStrategies.noRestart());
                        break;
                    case "fixed-delay":
                        int attempts = envConfig.getInt(ExecutionConfigKeyName.RESTART_ATTEMPTS);
                        long delay = envConfig.getLong(ExecutionConfigKeyName.RESTART_DELAY_BETWEEN_ATTEMPTS);
                        executionConfig.setRestartStrategy(
                                RestartStrategies.fixedDelayRestart(attempts, delay));
                        break;
                    case "failure-rate":
                        long failureInterval =
                                envConfig.getLong(ExecutionConfigKeyName.RESTART_FAILURE_INTERVAL);
                        int rate = envConfig.getInt(ExecutionConfigKeyName.RESTART_FAILURE_RATE);
                        long delayInterval = envConfig.getLong(ExecutionConfigKeyName.RESTART_DELAY_INTERVAL);
                        executionConfig.setRestartStrategy(
                                RestartStrategies.failureRateRestart(
                                        rate, // max failures per interval
                                        Time.of(failureInterval, TimeUnit.MILLISECONDS), //time interval for measuring failure rate
                                        Time.of(delayInterval, TimeUnit.MILLISECONDS))); // delay
                        break;
                    default:
                        log.warn(
                                "Set restart strategy failed, unknown restart strategy [{}],only support no,fixed-delay,failure-rate",
                                restartStrategy);
                }
            }
        } catch (Exception e) {
            log.warn("Set restart strategy in config '{}' exception", envConfig, e);
        }
    }

    public static CheckResult checkRestartStrategy(Config envConfig) {
        if (envConfig.hasPath(ExecutionConfigKeyName.RESTART_STRATEGY)) {
            String restartStrategy = envConfig.getString(ExecutionConfigKeyName.RESTART_STRATEGY);
            switch (restartStrategy.toLowerCase()) {
                case "fixed-delay":
                    if (!(envConfig.hasPath(ExecutionConfigKeyName.RESTART_ATTEMPTS)
                            && envConfig.hasPath(ExecutionConfigKeyName.RESTART_DELAY_BETWEEN_ATTEMPTS))) {
                        return CheckResult.error(
                                String.format(
                                        "fixed-delay restart strategy must set [%s],[%s]",
                                        ExecutionConfigKeyName.RESTART_ATTEMPTS,
                                        ExecutionConfigKeyName.RESTART_DELAY_BETWEEN_ATTEMPTS));
                    }
                    break;
                case "failure-rate":
                    if (!(envConfig.hasPath(ExecutionConfigKeyName.RESTART_FAILURE_INTERVAL)
                            && envConfig.hasPath(ExecutionConfigKeyName.RESTART_FAILURE_RATE)
                            && envConfig.hasPath(ExecutionConfigKeyName.RESTART_DELAY_INTERVAL))) {
                        return CheckResult.error(
                                String.format(
                                        "failure-rate restart strategy must set [%s],[%s],[%s]",
                                        ExecutionConfigKeyName.RESTART_FAILURE_INTERVAL,
                                        ExecutionConfigKeyName.RESTART_FAILURE_RATE,
                                        ExecutionConfigKeyName.RESTART_DELAY_INTERVAL));
                    }
                    break;
                default:
                    return CheckResult.success();
            }
        }
        return CheckResult.success();
    }


}
