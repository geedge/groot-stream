package com.geedgenetworks.bootstrap.execution;

public class ExecutionConfigKeyName {
    private ExecutionConfigKeyName() {
        throw new UnsupportedOperationException("Utility class should not be instantiated");
    }
    public static final String PARALLELISM = "parallelism";
    public static final String BUFFER_TIMEOUT_MILLIS = "execution.buffer-timeout";

    public static final String RUNTIME_MODE = "execution.runtime-mode";
    public static final String MAX_PARALLELISM = "pipeline.max-parallelism";
    public static final String TIME_CHARACTERISTIC = "pipeline.time-characteristic";
    public static final String CHECKPOINTING_INTERVAL = "execution.checkpointing.interval";
    public static final String CHECKPOINTING_MODE = "execution.checkpointing.mode";
    public static final String CHECKPOINTING_TIMEOUT = "execution.checkpointing.timeout";
    public static final String CHECKPOINTING_DATA_URI = "execution.checkpointing.data-uri";
    public static final String MAX_CONCURRENT_CHECKPOINTS = "execution.checkpointing.max-concurrent-checkpoints";
    public static final String CHECKPOINTING_CLEANUP = "execution.checkpointing.cleanup";
    public static final String MIN_PAUSE_BETWEEN_CHECKPOINTS = "execution.checkpointing.min-pause";
    public static final String TOLERABLE_FAILED_CHECKPOINTS = "execution.checkpointing.tolerable-failed-checkpoints";
    public static final String STATE_BACKEND = "state.backend";
    public static final String RESTART_STRATEGY = "execution.restart.strategy";
    public static final String RESTART_ATTEMPTS = "execution.restart.attempts";
    public static final String RESTART_DELAY_BETWEEN_ATTEMPTS = "execution.restart.delayBetweenAttempts";
    public static final String RESTART_FAILURE_INTERVAL = "execution.restart.failureInterval";
    public static final String RESTART_FAILURE_RATE = "execution.restart.failureRate";
    public static final String RESTART_DELAY_INTERVAL = "execution.restart.delayInterval";
    public static final String MAX_STATE_RETENTION_TIME = "execution.query.state.max-retention";
    public static final String MIN_STATE_RETENTION_TIME = "execution.query.state.min-retention";

    public static final String PLANNER = "execution.planner";
    //third-party packages can be loaded via `jars`
    public static final String JARS = "jars";
    public static final String ENV_TARGET_TYPE = "target_type";
    public static final String PIPELINE_JARS = "pipeline.jars";
    public static final String PIPELINE_CLASSPATHS = "pipeline.classpaths";

}
