package com.geedgenetworks.bootstrap.execution;

import com.alibaba.fastjson.JSONObject;
import com.geedgenetworks.bootstrap.enums.ProcessorType;
import com.geedgenetworks.bootstrap.exception.ConfigCheckException;
import com.geedgenetworks.bootstrap.exception.JobExecuteException;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.config.GrootYamlParser;
import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.SinkConfig;
import com.geedgenetworks.core.sink.Sink;
import com.geedgenetworks.utils.StringUtil;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.Map;

@Slf4j
public class SinkExecuteProcessor extends AbstractExecuteProcessor<String, SinkConfig>{
    private static final String PROCESSOR_TYPE = ProcessorType.SINK.getType();

    public SinkExecuteProcessor(Map<String, Object> configMap) {
        super(GrootYamlParser.getMap(Constants.SINKS, configMap));
    }
    @Override
    protected Map<String, SinkConfig> initialize(Map<String, Object> operatorConfigMap) {
        Map<String, SinkConfig> sinkConfigMap = Maps.newHashMap();
        if(operatorConfigMap == null) {
            return sinkConfigMap;
        }
        operatorConfigMap.forEach((key,value) -> {
            SinkConfig sinkConfig = new JSONObject((Map<String, Object>) value).toJavaObject(SinkConfig.class);
            checkSinkConfig(sinkConfig);
            sinkConfig.setName(key);
            sinkConfigMap.put(key, sinkConfig);
        });
        return sinkConfigMap;
    }

    @Override
    public SingleOutputStreamOperator execute(SingleOutputStreamOperator inputDataStream, Edge edge) throws JobExecuteException {

        SinkConfig sinkConfig = operatorMap.get(edge.getTarget());

        String className = sinkConfig.getType();
        SingleOutputStreamOperator<Event> singleOutputStreamOperator = null;
        Class cls = null;
        Sink sink = null;
        try {
            cls = Class.forName(className);
            sink = (Sink) cls.newInstance();
            if (edge.getParallelism() > 0) {
                sinkConfig.setParallelism(edge.getParallelism());
            }
            sink.sink(inputDataStream, sinkConfig);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private void checkSinkConfig(SinkConfig sinkConfig) {

        if (!StringUtil.isNotBlank(sinkConfig.getType())) {
            throw new ConfigCheckException("校验未通过 sink " + sinkConfig.getName() + " type 为空！");
        }
    }

}
