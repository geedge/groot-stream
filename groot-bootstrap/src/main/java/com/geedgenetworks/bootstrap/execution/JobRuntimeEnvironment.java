package com.geedgenetworks.bootstrap.execution;

import com.geedgenetworks.bootstrap.enums.TargetType;
import com.geedgenetworks.bootstrap.utils.EnvironmentUtil;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.config.CheckResult;
import com.geedgenetworks.common.utils.ReflectionUtils;
import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
public class JobRuntimeEnvironment implements RuntimeEnvironment{
    private static volatile JobRuntimeEnvironment INSTANCE = null;
    private Config appConfig;
    private StreamExecutionEnvironment environment;

    private JobRuntimeEnvironment(Config config) {
        this.initialize(config);
    }

    public static JobRuntimeEnvironment getInstance(Config config) {
        if (INSTANCE == null) {
            synchronized (JobRuntimeEnvironment.class) {
                if (INSTANCE == null) {
                    INSTANCE = new JobRuntimeEnvironment(config);
                }
            }
        }
        return INSTANCE;
    }

    @Override
    public RuntimeEnvironment setConfig(Config appConfig) {
        this.appConfig = appConfig;
        return this;
    }

    @Override
    public Config getConfig() {
        return appConfig;
    }

    @Override
    public CheckResult checkConfig() {
        return EnvironmentUtil.checkRestartStrategy(appConfig);
    }

    @Override
    public RuntimeEnvironment prepare() {
        createStreamEnvironment();

        return this;
    }

    @Override
    public void registerPlugin(List<URL> pluginPaths) {
        pluginPaths.forEach(url -> log.info("Begin register plugins: {}", url));
        List<Configuration> configurations = new ArrayList<>();
        try {
            configurations.add((Configuration) Objects.requireNonNull(ReflectionUtils
                    .getDeclaredMethod(StreamExecutionEnvironment.class, "getConfiguration"))
                    .orElseThrow(() -> new RuntimeException("can't find method : getConfiguration"))
                    .invoke(this.environment));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        configurations.forEach(configuration -> {
            List<String> jars = configuration.get(PipelineOptions.JARS);
            if (jars == null) {
                jars = new ArrayList<>();
            }
            jars.addAll(pluginPaths.stream().map(URL::toString).collect(Collectors.toList()));
            configuration.set(PipelineOptions.JARS, jars.stream().distinct().collect(Collectors.toList()));
            List<String> classpath = configuration.get(PipelineOptions.CLASSPATHS);
            if (classpath == null) {
                classpath = new ArrayList<>();
            }
            classpath.addAll(pluginPaths.stream().map(URL::toString).collect(Collectors.toList()));
            configuration.set(PipelineOptions.CLASSPATHS, classpath.stream().distinct().collect(Collectors.toList()));

        } );
    }

    public StreamExecutionEnvironment getStreamExecutionEnvironment() {
        return environment;
    }

    private void createStreamEnvironment() {

        Configuration configuration = new Configuration();
        //init GlobalJobParameters
        EnvironmentUtil.initConfiguration(appConfig, configuration);
        Config envConfig = appConfig.getConfig(Constants.APPLICATION_ENV);
        if (envConfig.hasPath(ExecutionConfigKey.ENV_TARGET_TYPE)
                && envConfig.getString(ExecutionConfigKey.ENV_TARGET_TYPE).equals(TargetType.LOCAL.getTarget())) {
            configuration.setInteger(RestOptions.PORT, 8082);
            environment  = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        } else {
            environment = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        }
        environment.getConfig().enableObjectReuse();
        setTimeCharacteristic();
        // setCheckpoint();
        EnvironmentUtil.setRestartStrategy(appConfig, environment.getConfig());
        if (envConfig.hasPath(ExecutionConfigKey.BUFFER_TIMEOUT_MILLIS)) {
            long timeout = envConfig.getLong(ExecutionConfigKey.BUFFER_TIMEOUT_MILLIS);
            environment.setBufferTimeout(timeout);
        }

        if (envConfig.hasPath(ExecutionConfigKey.PARALLELISM)) {
            int parallelism = envConfig.getInt(ExecutionConfigKey.PARALLELISM);
            environment.setParallelism(parallelism);
        }

    }

    private void setTimeCharacteristic() {
        if (appConfig.hasPath(ExecutionConfigKey.TIME_CHARACTERISTIC)) {
            String timeType = appConfig.getString(ExecutionConfigKey.TIME_CHARACTERISTIC);
            switch (timeType.toLowerCase()) {
                case "event-time":
                    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
                    break;
                case "ingestion-time":
                    environment.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
                    break;
                case "processing-time":
                    environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
                    break;
                default:
                    log.warn(
                            "set time-characteristic failed, unknown time-characteristic [{}],only support event-time,ingestion-time,processing-time",
                            timeType);
                    break;
            }
        }
    }




}








