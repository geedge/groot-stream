package com.geedgenetworks.bootstrap.execution;

import com.alibaba.fastjson2.JSON;
import com.geedgenetworks.bootstrap.enums.TargetType;
import com.geedgenetworks.bootstrap.utils.EnvironmentUtil;
import com.geedgenetworks.common.config.Constants;
import com.geedgenetworks.common.config.CheckResult;
import com.geedgenetworks.common.config.GrootStreamConfig;
import com.geedgenetworks.common.utils.ReflectionUtils;
import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.TernaryBoolean;

import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
public class JobRuntimeEnvironment implements RuntimeEnvironment{
    private static volatile JobRuntimeEnvironment INSTANCE = null;
    private Config envConfig;
    private GrootStreamConfig grootStreamConfig;
    private StreamExecutionEnvironment environment;
    private String jobName = Constants.DEFAULT_JOB_NAME;

    private JobRuntimeEnvironment(Config jobConfig, GrootStreamConfig grootStreamConfig) {
        this.grootStreamConfig = grootStreamConfig;
        this.initialize(jobConfig);
    }

    public static JobRuntimeEnvironment getInstance(Config jobConfig, GrootStreamConfig grootStreamConfig) {
        if (INSTANCE == null) {
            synchronized (JobRuntimeEnvironment.class) {
                if (INSTANCE == null) {
                    INSTANCE = new JobRuntimeEnvironment(jobConfig, grootStreamConfig);
                }
            }
        }
        return INSTANCE;
    }

    @Override
    public RuntimeEnvironment setEnvConfig(Config envConfig) {
        this.envConfig = envConfig;
        return this;
    }

    @Override
    public Config getEnvConfig() {
        return envConfig;
    }

    @Override
    public CheckResult checkConfig() {
        return EnvironmentUtil.checkRestartStrategy(envConfig);
    }


    @Override
    public RuntimeEnvironment prepare() {

        if (envConfig.hasPath(Constants.JOB_NAME)) {
            jobName = envConfig.getString(Constants.JOB_NAME);
        }
        // Job-level user-defined variables override the grootStreamConfig
        if (envConfig.hasPath(Constants.PROPERTIES)) {
            envConfig.getConfig(Constants.PROPERTIES).root().unwrapped().entrySet().forEach(entry -> {
                this.grootStreamConfig.getCommonConfig().getPropertiesConfig().put(entry.getKey(), String.valueOf(entry.getValue()));
            });
        }

        createStreamEnvironment();

        return this;
    }

    public String getJobName() {
        return jobName;
    }

    public boolean isLocalMode() {
        return envConfig.hasPath(ExecutionConfigKeyName.ENV_TARGET_TYPE)
                && envConfig.getString(ExecutionConfigKeyName.ENV_TARGET_TYPE).equals(TargetType.LOCAL.getTarget());
    }
    public boolean isTestMode() {
        return envConfig.hasPath(ExecutionConfigKeyName.ENV_TARGET_TYPE)
                && envConfig.getString(ExecutionConfigKeyName.ENV_TARGET_TYPE).equals(TargetType.TEST.getTarget());
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
        EnvironmentUtil.initConfiguration(envConfig, configuration);
        if (isLocalMode()) {
            configuration.setInteger(RestOptions.PORT, 8082);
            environment  = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        } else {
            environment = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        }
        configuration.setString(Constants.SYSPROP_UDF_PLUGIN_CONFIG, JSON.toJSONString(grootStreamConfig.getUDFPluginConfig()));
        configuration.setString(Constants.SYSPROP_GROOTSTREAM_CONFIG, JSON.toJSONString(grootStreamConfig.getCommonConfig()));
        environment.getConfig().setGlobalJobParameters(configuration);
        setTimeCharacteristic();
        setCheckpoint();
        setRuntimeMode();
        EnvironmentUtil.setRestartStrategy(envConfig, environment.getConfig());

        if (envConfig.hasPath(ExecutionConfigKeyName.BUFFER_TIMEOUT_MILLIS)) {
            long timeout = envConfig.getLong(ExecutionConfigKeyName.BUFFER_TIMEOUT_MILLIS);
            environment.setBufferTimeout(timeout);
        }

        if (envConfig.hasPath(ExecutionConfigKeyName.PARALLELISM)) {
            int parallelism = envConfig.getInt(ExecutionConfigKeyName.PARALLELISM);
            environment.setParallelism(parallelism);
        }

        if (envConfig.hasPath(ExecutionConfigKeyName.MAX_PARALLELISM)) {
            int maxParallelism = envConfig.getInt(ExecutionConfigKeyName.MAX_PARALLELISM);
            environment.setMaxParallelism(maxParallelism);
        }

    }

    private void setRuntimeMode() {
        if (envConfig.hasPath(ExecutionConfigKeyName.RUNTIME_MODE)) {
            String runtimeMode = envConfig.getString(ExecutionConfigKeyName.RUNTIME_MODE);
            if (runtimeMode.equalsIgnoreCase("batch")) {
                environment.setRuntimeMode(RuntimeExecutionMode.BATCH);
            } else if (runtimeMode.equalsIgnoreCase("streaming")) {
                environment.setRuntimeMode(RuntimeExecutionMode.STREAMING);
            } else {
                log.warn(
                        "set runtime-mode failed, unknown runtime-mode [{}],only support batch,streaming",
                        runtimeMode);
            }
        }
    }

    private void setTimeCharacteristic() {
        if (envConfig.hasPath(ExecutionConfigKeyName.TIME_CHARACTERISTIC)) {
            String timeType = envConfig.getString(ExecutionConfigKeyName.TIME_CHARACTERISTIC);
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

    private void setCheckpoint() {
        long interval = 0;
        if (envConfig.hasPath(ExecutionConfigKeyName.CHECKPOINTING_INTERVAL)) {
            interval = envConfig.getLong(ExecutionConfigKeyName.CHECKPOINTING_INTERVAL);
        }

        if (interval > 0) {
            CheckpointConfig checkpointConfig = environment.getCheckpointConfig();
            environment.enableCheckpointing(interval);
            if (envConfig.hasPath(ExecutionConfigKeyName.CHECKPOINTING_MODE)) {
                String mode = envConfig.getString(ExecutionConfigKeyName.CHECKPOINTING_MODE);
                switch (mode.toLowerCase()) {
                    case "exactly-once":
                        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
                        break;
                    case "at-least-once":
                        checkpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
                        break;
                    default:
                        log.warn(
                                "set checkpointing.mode failed, unknown checkpointing.mode [{}],only support exactly-once,at-least-once",
                                mode);
                        break;
                }
            }

            if (envConfig.hasPath(ExecutionConfigKeyName.CHECKPOINTING_TIMEOUT)) {
                long timeout = envConfig.getLong(ExecutionConfigKeyName.CHECKPOINTING_TIMEOUT);
                checkpointConfig.setCheckpointTimeout(timeout);
            }

            if (envConfig.hasPath(ExecutionConfigKeyName.CHECKPOINTING_DATA_URI)) {
                String uri = envConfig.getString(ExecutionConfigKeyName.CHECKPOINTING_DATA_URI);
                StateBackend fsStateBackend = new FsStateBackend(uri);
                if (envConfig.hasPath(ExecutionConfigKeyName.STATE_BACKEND)) {
                    String stateBackend = envConfig.getString(ExecutionConfigKeyName.STATE_BACKEND);
                    if ("rocksdb".equalsIgnoreCase(stateBackend)) {
                        StateBackend rocksDBStateBackend =
                                new RocksDBStateBackend(fsStateBackend, TernaryBoolean.TRUE);
                        environment.setStateBackend(rocksDBStateBackend);
                    }
                } else {
                    environment.setStateBackend(fsStateBackend);
                }
            }

            if (envConfig.hasPath(ExecutionConfigKeyName.MAX_CONCURRENT_CHECKPOINTS)) {
                int max = envConfig.getInt(ExecutionConfigKeyName.MAX_CONCURRENT_CHECKPOINTS);
                checkpointConfig.setMaxConcurrentCheckpoints(max);
            }

            if (envConfig.hasPath(ExecutionConfigKeyName.CHECKPOINTING_CLEANUP)) {
                boolean cleanup = envConfig.getBoolean(ExecutionConfigKeyName.CHECKPOINTING_CLEANUP);
                if (cleanup) {
                    checkpointConfig.enableExternalizedCheckpoints(
                            CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
                } else {
                    checkpointConfig.enableExternalizedCheckpoints(
                            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
                }
            }

            if (envConfig.hasPath(ExecutionConfigKeyName.MIN_PAUSE_BETWEEN_CHECKPOINTS)) {
                long minPause = envConfig.getLong(ExecutionConfigKeyName.MIN_PAUSE_BETWEEN_CHECKPOINTS);
                checkpointConfig.setMinPauseBetweenCheckpoints(minPause);
            }

            if (envConfig.hasPath(ExecutionConfigKeyName.TOLERABLE_FAILED_CHECKPOINTS)) {
                int failNum = envConfig.getInt(ExecutionConfigKeyName.TOLERABLE_FAILED_CHECKPOINTS);
                checkpointConfig.setTolerableCheckpointFailureNumber(failNum);
            }
        }




    }




}








