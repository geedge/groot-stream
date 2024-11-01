package com.geedgenetworks.core.processor.projection;

import com.alibaba.fastjson.JSONObject;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.config.CheckConfigUtil;
import com.geedgenetworks.common.config.CheckResult;
import com.geedgenetworks.common.config.ProjectionConfigOptions;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.ConfigValidationException;
import com.geedgenetworks.core.pojo.ProjectionConfig;

import com.typesafe.config.Config;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;

import java.util.Map;

public class ProjectionProcessorImpl implements ProjectionProcessor {


    @Override
    public DataStream<Event> processorFunction(DataStream<Event> grootEventDataStream, ProjectionConfig projectionConfig, ExecutionConfig config) throws Exception {
        if (projectionConfig.getParallelism() != 0) {
            return grootEventDataStream
                    .process(new ProjectionProcessFunction(projectionConfig))
                    .setParallelism(projectionConfig.getParallelism())
                    .name(projectionConfig.getName());
        } else {
            return grootEventDataStream
                    .process(new ProjectionProcessFunction(projectionConfig))
                    .name(projectionConfig.getName());
        }
    }
    @Override
    public String type() {
        return "projection";
    }

    @Override
    public ProjectionConfig checkConfig(String name, Map<String, Object> configProperties, Config typeSafeConfig) {
        CheckResult result = CheckConfigUtil.checkAtLeastOneExists(typeSafeConfig.getConfig(name),
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

        ProjectionConfig projectionConfig = new JSONObject(configProperties).toJavaObject(ProjectionConfig.class);
        projectionConfig.setName(name);

        return projectionConfig;
    }
}
