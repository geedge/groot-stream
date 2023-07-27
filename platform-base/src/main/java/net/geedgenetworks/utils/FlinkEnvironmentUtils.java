package net.geedgenetworks.utils;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;


/**
 * @author wlh
 */
public class FlinkEnvironmentUtils {
    public static StreamExecutionEnvironment streamExeEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    public static StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(streamExeEnv);

    static {
      /*  streamExeEnv.setParallelism(CommonConfig.STREAM_EXECUTION_ENVIRONMENT_PARALLELISM);
        //checkpoint配置

        switch (CommonConfig.FLINK_ENABLE_CHECKPOINT_FLAG) {
            case "0":
                break;
            case "1":
                streamExeEnv.enableCheckpointing(CommonConfig.FLINK_ENABLE_CHECKPOINT_TIME * 1000);
                CheckpointConfig checkpointConfig = streamExeEnv.getCheckpointConfig();
                checkpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
                checkpointConfig.setMinPauseBetweenCheckpoints(CommonConfig.FLINK_MIN_PAUSE_BETWEEN_CHECKPOINTS * 1000);
                checkpointConfig.setCheckpointTimeout(CommonConfig.FLINK_CHECKPOINT_TIMEOUT * 1000);
                checkpointConfig.setTolerableCheckpointFailureNumber(CommonConfig.FLINK_TOLERABLE_CHECKPOINT_FAILURE_NUMBER);
                checkpointConfig.setMaxConcurrentCheckpoints(CommonConfig.FLINK_MAX_CONCURRENT_CHECKPOINT);
                checkpointConfig.enableUnalignedCheckpoints();
                break;
            default:
                throw new IllegalArgumentException();
        }*/

    }

    public static <T> WatermarkStrategy<T> createWatermarkStrategy(Duration maxOutOfOrderness, SerializableTimestampAssigner<T> timestampAssigner) {
        return WatermarkStrategy
                .<T>forBoundedOutOfOrderness(maxOutOfOrderness)
                .withTimestampAssigner(timestampAssigner);
    }

    /*
    public static StreamTableEnvironment getTableEnv() {
        if (tableEnvironment == null) {
            tableEnvironment = StreamTableEnvironment.create(streamExeEnv);
        }
        return tableEnvironment;
    }
    */

}
