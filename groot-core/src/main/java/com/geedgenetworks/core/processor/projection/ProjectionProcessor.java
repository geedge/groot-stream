package com.geedgenetworks.core.processor.projection;

import com.alibaba.fastjson.JSONObject;
import com.geedgenetworks.common.config.CheckConfigUtil;
import com.geedgenetworks.common.config.CheckResult;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.ConfigValidationException;

import com.geedgenetworks.spi.configuration.ProjectionConfigOptions;
import com.geedgenetworks.spi.processor.Processor;
import com.geedgenetworks.spi.processor.ProjectionConfig;
import com.geedgenetworks.spi.table.event.Event;
import com.typesafe.config.Config;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

public class ProjectionProcessor implements Processor<ProjectionConfig> {


    @Override
    public DataStream<Event> process(StreamExecutionEnvironment env, DataStream<Event> input, ProjectionConfig projectionConfig) {
        if (projectionConfig.getParallelism() != 0) {
            return input
                    .process(new ProjectionProcessFunction(projectionConfig))
                    .setParallelism(projectionConfig.getParallelism())
                    .name(projectionConfig.getName());
        } else {
            return input
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
