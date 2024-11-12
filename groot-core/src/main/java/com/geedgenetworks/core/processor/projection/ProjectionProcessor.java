package com.geedgenetworks.core.processor.projection;

import com.alibaba.fastjson.JSONObject;
import com.geedgenetworks.common.config.CheckConfigUtil;
import com.geedgenetworks.common.config.CheckResult;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.ConfigValidationException;
import com.geedgenetworks.api.processor.Processor;
import com.geedgenetworks.api.connector.event.Event;
import com.typesafe.config.Config;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ProjectionProcessor implements Processor<ProjectionConfig> {

    @Override
    public DataStream<Event> process(StreamExecutionEnvironment env,
                                     DataStream<Event> input, ProjectionConfig projectionConfig) {
        SingleOutputStreamOperator<Event> resultStream  =
                input.process(new ProjectionProcessFunction(projectionConfig)).name(projectionConfig.getName());

        if (projectionConfig.getParallelism() != 0) {
            resultStream.setParallelism(projectionConfig.getParallelism());
        }
        return resultStream;
    }

    @Override
    public ProjectionConfig parseConfig(String name, Config config) {

        CheckResult result = CheckConfigUtil.checkAtLeastOneExists(config,
                ProjectionConfigOptions.OUTPUT_FIELDS.key(),
                ProjectionConfigOptions.REMOVE_FIELDS.key(),
                ProjectionConfigOptions.FUNCTIONS.key());
        if (!result.isSuccess()) {
            throw new ConfigValidationException(CommonErrorCode.CONFIG_VALIDATION_FAILED, String.format(
                    "Processor: %s, At least one of [%s] should be specified.",
                    name, String.join(",",
                            ProjectionConfigOptions.OUTPUT_FIELDS.key(),
                            ProjectionConfigOptions.REMOVE_FIELDS.key(),
                            ProjectionConfigOptions.FUNCTIONS.key())));
        }

        ProjectionConfig projectionConfig = new JSONObject(config.root().unwrapped()).toJavaObject(ProjectionConfig.class);
        projectionConfig.setName(name);

        return projectionConfig;
    }
}
