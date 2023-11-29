package com.geedgenetworks.bootstrap.execution;

import com.alibaba.fastjson.JSONObject;
import com.geedgenetworks.bootstrap.enums.ProcessorType;
import com.geedgenetworks.bootstrap.exception.ConfigCheckException;
import com.geedgenetworks.bootstrap.exception.JobExecuteException;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.core.pojo.ProjectionConfig;
import com.geedgenetworks.core.processor.projection.ProjectionProcessor;
import com.geedgenetworks.utils.StringUtil;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.net.URL;
import java.util.List;
import java.util.Map;

public class PostprocessingExecuteProcessor extends AbstractExecuteProcessor<String, ProjectionConfig> {
    private static final String PROCESSOR_TYPE = ProcessorType.POSTPROCESSING.getType();

    public PostprocessingExecuteProcessor(List<URL> jarPaths, Config config) {
        super(jarPaths, config);
    }
    @Override
    protected Map<String, ProjectionConfig> initialize(List<URL> jarPaths, Config operatorConfig) {
        Map<String, ProjectionConfig> postprocessingConfigMap = Maps.newHashMap();
        if (operatorConfig.hasPath(Constants.POSTPROCESSING_PIPELINES)) {
            Map<String, Object>  operatorConfigMap = operatorConfig.getConfig(Constants.POSTPROCESSING_PIPELINES).root().unwrapped();
            operatorConfigMap.forEach((key,value) -> {
                ProjectionConfig projectionConfig = new JSONObject((Map<String, Object>) value).toJavaObject(ProjectionConfig.class);
                checkProjectionConfig(projectionConfig);
                projectionConfig.setName(key);
                postprocessingConfigMap.put(key, projectionConfig);
            });
        }

        return postprocessingConfigMap;
    }

    @Override
    public SingleOutputStreamOperator execute(SingleOutputStreamOperator dataStream, Node node) throws JobExecuteException {
        Class cls = null;
        ProjectionProcessor projectionProcessor = null;
        ProjectionConfig projectionConfig = operatorMap.get(node.getName());
        try {
            cls = Class.forName(projectionConfig.getType());
            projectionProcessor = (ProjectionProcessor) cls.newInstance();

            if (node.getParallelism() > 0) {
                projectionConfig.setParallelism(node.getParallelism());
            }
            dataStream =
                    projectionProcessor.projectionProcessorFunction(
                            dataStream, projectionConfig);
        } catch (Exception e) {
            throw new JobExecuteException("create postprocessing operator error", e);
        }
        return dataStream;
    }
    private static void checkProjectionConfig(ProjectionConfig projectionConfig) {

        if (!StringUtil.isNotBlank(projectionConfig.getType())) {
            throw new ConfigCheckException("校验未通过 Processing Pipelines " + projectionConfig.getName() + " type 为空！");
        }
    }
}
