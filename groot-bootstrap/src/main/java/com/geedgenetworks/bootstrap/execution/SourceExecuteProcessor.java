package com.geedgenetworks.bootstrap.execution;

import com.alibaba.fastjson.JSONObject;
import com.geedgenetworks.bootstrap.enums.ProcessorType;
import com.geedgenetworks.bootstrap.exception.ConfigCheckException;
import com.geedgenetworks.bootstrap.exception.JobExecuteException;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.core.pojo.SourceConfig;
import com.geedgenetworks.core.source.Source;
import com.geedgenetworks.utils.StringUtil;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.net.URL;
import java.util.List;
import java.util.Map;

@Slf4j
public class SourceExecuteProcessor extends AbstractExecuteProcessor<String,SourceConfig> {
    private static final String PROCESSOR_TYPE = ProcessorType.SOURCE.getType();

    public SourceExecuteProcessor(List<URL> jarPaths, Config config) {
        super(jarPaths, config);
    }
    @Override
    protected Map<String, SourceConfig> initialize(List<URL> jarPaths, Config operatorConfig) {
        Map<String, SourceConfig> sourceConfigMap = Maps.newHashMap();
        if (operatorConfig.hasPath(Constants.SOURCES)) {
            Map<String, Object>  operatorConfigMap = operatorConfig.getConfig(Constants.SOURCES).root().unwrapped();
            operatorConfigMap.forEach((key,value) -> {
                SourceConfig sourceConfig = new JSONObject((Map<String, Object>) value).toJavaObject(SourceConfig.class);
                checkSourceConfig(sourceConfig);
                sourceConfig.setName(key);
                sourceConfigMap.put(key, sourceConfig);
            });
        }

        return sourceConfigMap;
    }

    @Override
    public SingleOutputStreamOperator execute(SingleOutputStreamOperator dataStream, Node node) throws JobExecuteException {
        SourceConfig sourceConfig = operatorMap.get(node.getName());

        try {
            Class cls = Class.forName(sourceConfig.getType());
            Source source = (Source) cls.newInstance();
            if (node.getParallelism() >0) {
                sourceConfig.setParallelism(node.getParallelism());
            }
            dataStream =
                    source.source(jobRuntimeEnvironment.getStreamExecutionEnvironment(), sourceConfig);
        } catch (Exception e) {
            throw new JobExecuteException("create source operator error", e);
        }
        return dataStream;
    }
    private void checkSourceConfig(SourceConfig sourceConfig) {
        if (!StringUtil.isNotBlank(sourceConfig.getType())) {
            throw new ConfigCheckException("校验未通过 source " + sourceConfig.getName() + " type 为空！");
        }
    }
}
