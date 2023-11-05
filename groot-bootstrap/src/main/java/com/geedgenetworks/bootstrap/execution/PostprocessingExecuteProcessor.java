package com.geedgenetworks.bootstrap.execution;

import com.alibaba.fastjson.JSONObject;
import com.geedgenetworks.bootstrap.enums.ProcessorType;
import com.geedgenetworks.bootstrap.exception.ConfigCheckException;
import com.geedgenetworks.bootstrap.exception.JobExecuteException;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.config.GrootYamlParser;
import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.ProjectionConfig;
import com.geedgenetworks.core.processor.projection.ProjectionProcessor;
import com.geedgenetworks.utils.StringUtil;
import com.google.common.collect.Maps;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.Map;

public class PostprocessingExecuteProcessor extends AbstractExecuteProcessor<String, ProjectionConfig> {
    private static final String PROCESSOR_TYPE = ProcessorType.POSTPROCESSING.getType();

    public PostprocessingExecuteProcessor(Map<String, Object> configMap) {
        super(GrootYamlParser.getMap(Constants.POSTPROCESSING_PIPELINES, configMap));
    }
    @Override
    protected Map<String, ProjectionConfig> initialize(Map<String, Object> operatorConfigMap) {
        Map<String, ProjectionConfig> postprocessingConfigMap = Maps.newHashMap();
        if(operatorConfigMap == null) {
            return postprocessingConfigMap;
        }
        operatorConfigMap.forEach((key,value) -> {
            ProjectionConfig projectionConfig = new JSONObject((Map<String, Object>) value).toJavaObject(ProjectionConfig.class);
            checkProjectionConfig(projectionConfig);
            projectionConfig.setName(key);
            postprocessingConfigMap.put(key, projectionConfig);
        });
        return postprocessingConfigMap;
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
            throw new JobExecuteException("create postprocessing operator error", e);
        }
        return singleOutputStreamOperator;
    }
    private static void checkProjectionConfig(ProjectionConfig projectionConfig) {

        if (!StringUtil.isNotBlank(projectionConfig.getType())) {
            throw new ConfigCheckException("校验未通过 Processing Pipelines " + projectionConfig.getName() + " type 为空！");
        }
    }
}
