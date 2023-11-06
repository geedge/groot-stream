package com.geedgenetworks.bootstrap.execution;

import cn.hutool.core.map.MapUtil;
import com.geedgenetworks.bootstrap.enums.TargetType;
import com.geedgenetworks.bootstrap.utils.EnvironmentUtil;
import com.geedgenetworks.bootstrap.utils.ExecutionConfigKey;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.config.CheckResult;
import com.geedgenetworks.common.config.GrootYamlParser;
import com.geedgenetworks.common.utils.ReflectionUtils;
import com.geedgenetworks.utils.StringUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class JobRuntimeEnvironment implements RuntimeEnvironment{
    private static volatile JobRuntimeEnvironment INSTANCE = null;
    private Map<String, Object> appConfig;
    private StreamExecutionEnvironment environment;

    private JobRuntimeEnvironment(Map<String, Object> appConfig) {
        this.initialize(appConfig);
    }

    public static JobRuntimeEnvironment getInstance(Map<String, Object> appConfig) {
        if (INSTANCE == null) {
            synchronized (JobRuntimeEnvironment.class) {
                if (INSTANCE == null) {
                    INSTANCE = new JobRuntimeEnvironment(appConfig);
                }
            }
        }
        return INSTANCE;
    }

    @Override
    public RuntimeEnvironment setConfig(Map<String, Object> appConfig) {
        this.appConfig = appConfig;
        return this;
    }

    @Override
    public Map<String, Object> getConfig() {
        return appConfig;
    }

    @Override
    public CheckResult checkConfig() {
        //todo
        return null;
    }

    @Override
    public RuntimeEnvironment prepare() {
        createStreamEnvironment();
        return this;
    }

    @Override
    public void registerPlugin(List<URL> pluginPaths) {

        pluginPaths.forEach(url -> log.info("register plugins: {}", url));
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
        Map<String, Object> options = GrootYamlParser.getMap(Constants.APPLICATION_ENV_OPTIONS,appConfig);
        Optional<Object> optionalValue =  Optional.ofNullable(options.get(ExecutionConfigKey.ENV_TARGET_TYPE));

        if (optionalValue.isPresent() && optionalValue.get().equals(TargetType.LOCAL.getTarget())) {
            configuration.setInteger(RestOptions.PORT, 8082);
            environment  = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        } else {
            environment = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        }
        environment.getConfig().enableObjectReuse();
        // setTimeCharacteristic();
        // setCheckpoint();
        // EnvironmentUtil.setRestartStrategy(appConfig, environment.getConfig());
        if(MapUtil.isNotEmpty(options)) {
            if (StringUtil.isNotEmpty(options.get(ExecutionConfigKey.BUFFER_TIMEOUT_MILLIS))) {
                long timeout = MapUtil.getLong(options, ExecutionConfigKey.BUFFER_TIMEOUT_MILLIS);
                environment.setBufferTimeout(timeout);
            }
            if (StringUtil.isNotEmpty(options.get(ExecutionConfigKey.PARALLELISM))) {
                int parallelism = MapUtil.getInt(options, ExecutionConfigKey.PARALLELISM);
                environment.setParallelism(parallelism);
            }
        }

    }




}








