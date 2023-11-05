package com.geedgenetworks.bootstrap.utils;

public class ExecutionConfigKey {
    private ExecutionConfigKey() {
        throw new UnsupportedOperationException("ExecutionConfigKey is a utility class and cannot be instantiated");
    }

    public static final String TIME_CHARACTERISTIC = "execution.time-characteristic";
    public static final String BUFFER_TIMEOUT_MILLIS = "execution.buffer.timeout";
    public static final String PARALLELISM = "execution.parallelism";
    public static final String MAX_PARALLELISM = "execution.max-parallelism";
    public static final String CHECKPOINT_MODE = "execution.checkpoint.mode";
    public static final String CHECKPOINT_TIMEOUT = "execution.checkpoint.timeout";
    public static final String CHECKPOINT_DATA_URI = "execution.checkpoint.data-uri";
    public static final String MAX_CONCURRENT_CHECKPOINTS = "execution.max-concurrent-checkpoints";
    public static final String CHECKPOINT_CLEANUP_MODE = "execution.checkpoint.cleanup-mode";
    public static final String MIN_PAUSE_BETWEEN_CHECKPOINTS = "execution.checkpoint.min-pause";
    public static final String FAIL_ON_CHECKPOINTING_ERRORS = "execution.checkpoint.fail-on-error";
    public static final String RESTART_STRATEGY = "execution.restart.strategy";
    public static final String RESTART_ATTEMPTS = "execution.restart.attempts";
    public static final String RESTART_DELAY_BETWEEN_ATTEMPTS = "execution.restart.delayBetweenAttempts";
    public static final String RESTART_FAILURE_INTERVAL = "execution.restart.failureInterval";
    public static final String RESTART_FAILURE_RATE = "execution.restart.failureRate";
    public static final String RESTART_DELAY_INTERVAL = "execution.restart.delayInterval";
    public static final String MAX_STATE_RETENTION_TIME = "execution.query.state.max-retention";
    public static final String MIN_STATE_RETENTION_TIME = "execution.query.state.min-retention";
    public static final String STATE_BACKEND = "execution.state.backend";
    public static final String PLANNER = "execution.planner";
    //third-party packages can be loaded via `jars`
    public static final String JARS = "jars";
    public static final String ENV_TARGET_TYPE = "target_type";
    public static final String PIPELINE_JARS = "pipeline.jars";
    public static final String PIPELINE_CLASSPATHS = "pipeline.classpaths";

}
