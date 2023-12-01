package com.geedgenetworks.bootstrap.execution;

import com.alibaba.fastjson.JSONObject;
import com.geedgenetworks.bootstrap.enums.ProcessorType;
import com.geedgenetworks.bootstrap.exception.ConfigCheckException;
import com.geedgenetworks.bootstrap.exception.JobExecuteException;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.core.connector.sink.SinkProvider;
import com.geedgenetworks.core.factories.FactoryUtil;
import com.geedgenetworks.core.factories.SinkTableFactory;
import com.geedgenetworks.core.factories.TableFactory;
import com.geedgenetworks.core.pojo.SinkConfig;
import com.geedgenetworks.utils.StringUtil;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.net.URL;
import java.util.List;
import java.util.Map;

@Slf4j
public class SinkExecuteProcessor extends AbstractExecuteProcessor<String, SinkConfig>{
    private static final String PROCESSOR_TYPE = ProcessorType.SINK.getType();

    public SinkExecuteProcessor(List<URL> jarPaths, Config config) {
        super(jarPaths, config);
    }
    @Override
    protected Map<String, SinkConfig> initialize(List<URL> jarPaths, Config operatorConfig) {
        Map<String, SinkConfig> sinkConfigMap = Maps.newHashMap();
        if (operatorConfig.hasPath(Constants.SINKS)) {
            Map<String, Object>  operatorConfigMap = operatorConfig.getConfig(Constants.SINKS).root().unwrapped();
            operatorConfigMap.forEach((key,value) -> {
                SinkConfig sinkConfig = new JSONObject((Map<String, Object>) value).toJavaObject(SinkConfig.class);
                checkSinkConfig(sinkConfig);
                sinkConfig.setName(key);
                sinkConfigMap.put(key, sinkConfig);
            });
        }

        return sinkConfigMap;
    }

    @Override
    public SingleOutputStreamOperator execute(SingleOutputStreamOperator dataStream, Node node) throws JobExecuteException {
        SinkConfig sinkConfig = operatorMap.get(node.getName());
        try {
            SinkTableFactory sinkTableFactory = FactoryUtil.discoverTableFactory(SinkTableFactory.class, sinkConfig.getType());
            Map<String, String> options = sinkConfig.getProperties();
            Configuration configuration = Configuration.fromMap(options);
            TableFactory.Context context = new TableFactory.Context(null, null, options, configuration);
            SinkProvider sinkProvider = sinkTableFactory.getSinkProvider(context);
            DataStreamSink<?> dataStreamSink = sinkProvider.consumeDataStream(dataStream);
            if (node.getParallelism() > 0) {
                dataStreamSink.setParallelism(node.getParallelism());
            }
            dataStreamSink.name(sinkConfig.getName());
        } catch (Exception e) {
            throw new JobExecuteException("create sink operator error", e);
        }

        return null;
    }

    private void checkSinkConfig(SinkConfig sinkConfig) {

        if (!StringUtil.isNotBlank(sinkConfig.getType())) {
            throw new ConfigCheckException("校验未通过 sink " + sinkConfig.getName() + " type 为空！");
        }
    }

}
