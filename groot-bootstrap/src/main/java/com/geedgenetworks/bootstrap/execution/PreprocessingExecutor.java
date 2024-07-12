package com.geedgenetworks.bootstrap.execution;

import com.alibaba.fastjson.JSONObject;
import com.geedgenetworks.bootstrap.enums.ProcessorType;
import com.geedgenetworks.bootstrap.exception.JobExecuteException;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.config.CheckConfigUtil;
import com.geedgenetworks.common.config.CheckResult;
import com.geedgenetworks.common.config.ProjectionConfigOptions;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.ConfigValidationException;
import com.geedgenetworks.core.pojo.ProjectionConfig;
import com.geedgenetworks.core.processor.projection.ProjectionProcessor;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * Initialize config and execute preprocessor
 */
@Slf4j
public class PreprocessingExecutor extends AbstractExecutor<String, ProjectionConfig> {
    private static final String PROCESSOR_TYPE = ProcessorType.PREPROCESSING.getType();

    public PreprocessingExecutor(List<URL> jarPaths, Config config) {
        super(jarPaths, config);
    }

    @Override
    protected Map<String, ProjectionConfig> initialize(List<URL> jarPaths, Config operatorConfig) {
        Map<String, ProjectionConfig> preprocessingConfigMap = Maps.newHashMap();
        if (operatorConfig.hasPath(Constants.PREPROCESSING_PIPELINES)) {
            Config preprocessors = operatorConfig.getConfig(Constants.PREPROCESSING_PIPELINES);
            preprocessors.root().unwrapped().forEach((key, value) -> {
                CheckResult result = CheckConfigUtil.checkAllExists(preprocessors.getConfig(key),
                        ProjectionConfigOptions.TYPE.key());
                if (!result.isSuccess()) {
                    throw new ConfigValidationException(CommonErrorCode.CONFIG_VALIDATION_FAILED, String.format(
                            "Preprocessor: %s, Message: %s",
                            key, result.getMsg()));
                }

                result = CheckConfigUtil.checkAtLeastOneExists(preprocessors.getConfig(key),
                        ProjectionConfigOptions.OUTPUT_FIELDS.key(),
                        ProjectionConfigOptions.REMOVE_FIELDS.key(),
                        ProjectionConfigOptions.FUNCTIONS.key());
                if (!result.isSuccess()) {
                    throw new ConfigValidationException(CommonErrorCode.CONFIG_VALIDATION_FAILED, String.format(
                            "Preprocessor: %s, At least one of [%s] should be specified.",
                            key, String.join(",",
                                    ProjectionConfigOptions.OUTPUT_FIELDS.key(),
                                    ProjectionConfigOptions.REMOVE_FIELDS.key(),
                                    ProjectionConfigOptions.FUNCTIONS.key())));
                }

                ProjectionConfig projectionConfig = new JSONObject((Map<String, Object>) value).toJavaObject(ProjectionConfig.class);
                projectionConfig.setName(key);
                preprocessingConfigMap.put(key, projectionConfig);
            });
        }

        return preprocessingConfigMap;
    }

    @Override
    public SingleOutputStreamOperator execute(SingleOutputStreamOperator dataStream, Node node) throws JobExecuteException {
        ProjectionConfig projectionConfig = operatorMap.get(node.getName());
        String className = projectionConfig.getType();
        ProjectionProcessor projectionProcessor;
        if (projectionProcessorMap.containsKey(projectionConfig.getType())) {
            projectionProcessor = projectionProcessorMap.get(projectionConfig.getType());
        } else {
            Class cls;
            try {
                cls = Class.forName(className);
                projectionProcessor = (ProjectionProcessor) cls.newInstance();
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | RuntimeException e) {
                throw new JobExecuteException("get preprocessing pipeline instance failed!", e);
            }
        }
        if (node.getParallelism() > 0) {
            projectionConfig.setParallelism(node.getParallelism());
        }
        try {
            dataStream =
                    projectionProcessor.projectionProcessorFunction(
                            dataStream, projectionConfig);
        } catch (Exception e) {
            throw new JobExecuteException("Create preprocessing pipeline instance failed!", e);
        }
        return dataStream;
    }
}
