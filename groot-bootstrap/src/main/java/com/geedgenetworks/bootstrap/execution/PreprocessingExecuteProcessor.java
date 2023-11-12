package com.geedgenetworks.bootstrap.execution;

import com.alibaba.fastjson.JSONObject;
import com.geedgenetworks.bootstrap.enums.ProcessorType;
import com.geedgenetworks.bootstrap.exception.ConfigCheckException;
import com.geedgenetworks.bootstrap.exception.JobExecuteException;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.ProjectionConfig;
import com.geedgenetworks.core.processor.projection.ProjectionProcessor;
import com.geedgenetworks.utils.StringUtil;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.net.URL;
import java.util.List;
import java.util.Map;

public class PreprocessingExecuteProcessor extends AbstractExecuteProcessor<String, ProjectionConfig> {
    private static final String PROCESSOR_TYPE = ProcessorType.PREPROCESSING.getType();

    public PreprocessingExecuteProcessor(List<URL> jarPaths, Config config) {
        super(jarPaths, config);
    }
    @Override
    protected Map<String, ProjectionConfig> initialize(List<URL> jarPaths, Config operatorConfig) {
        Map<String, ProjectionConfig> preprocessingConfigMap = Maps.newHashMap();
        if (operatorConfig.hasPath(Constants.PREPROCESSING_PIPELINES)) {
            Map<String, Object>  operatorConfigMap = operatorConfig.getConfig(Constants.PREPROCESSING_PIPELINES).root().unwrapped();
            operatorConfigMap.forEach((key,value) -> {
                ProjectionConfig projectionConfig = new JSONObject((Map<String, Object>) value).toJavaObject(ProjectionConfig.class);
                checkProjectionConfig(projectionConfig);
                projectionConfig.setName(key);
                preprocessingConfigMap.put(key, projectionConfig);
            });
        }

        return preprocessingConfigMap;
    }

    @Override
    public SingleOutputStreamOperator execute(SingleOutputStreamOperator inputDataStream, Edge edge) throws JobExecuteException {

        Class cls = null;
        ProjectionProcessor projectionProcessor = null;
        SingleOutputStreamOperator<Event> singleOutputStreamOperator = null;
        ProjectionConfig projectionConfig = operatorMap.get(edge.getInput());
        try {
            cls = Class.forName(projectionConfig.getType());
            projectionProcessor = (ProjectionProcessor) cls.newInstance();

            if (edge.getParallelism() > 0) {
                projectionConfig.setParallelism(edge.getParallelism());
            }
            singleOutputStreamOperator =
                    projectionProcessor.projectionProcessorFunction(
                            inputDataStream, projectionConfig);
        } catch (Exception e) {
            throw new JobExecuteException("create preprocessing operator error", e);
        }
        return singleOutputStreamOperator;
    }
    private static void checkProjectionConfig(ProjectionConfig projectionConfig) {

        if (!StringUtil.isNotBlank(projectionConfig.getType())) {
            throw new ConfigCheckException("校验未通过 projection " + projectionConfig.getName() + " type 为空！");
        }
    }
}
