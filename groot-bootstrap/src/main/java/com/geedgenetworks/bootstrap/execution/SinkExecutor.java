package com.geedgenetworks.bootstrap.execution;

import com.alibaba.fastjson.JSONObject;
import com.geedgenetworks.bootstrap.enums.StageType;
import com.geedgenetworks.bootstrap.exception.JobExecuteException;
import com.geedgenetworks.bootstrap.utils.SchemaConfigParse;
import com.geedgenetworks.common.config.Constants;
import com.geedgenetworks.common.config.CheckConfigUtil;
import com.geedgenetworks.common.config.CheckResult;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.ConfigValidationException;
import com.geedgenetworks.spi.sink.SinkConfig;
import com.geedgenetworks.spi.sink.SinkConfigOptions;
import com.geedgenetworks.spi.sink.SinkProvider;
import com.geedgenetworks.spi.sink.SinkTableFactory;
import com.geedgenetworks.spi.table.event.Event;
import com.geedgenetworks.spi.table.factory.FactoryUtil;
import com.geedgenetworks.spi.table.factory.TableFactory;
import com.geedgenetworks.spi.table.schema.Schema;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;

import java.util.Map;

/**
 * Initialize config and execute sink connector
 */
@Slf4j
public class SinkExecutor extends AbstractExecutor<String, SinkConfig> {
    private static final String PROCESSOR_TYPE = StageType.SINK.getType();

    public SinkExecutor(Config config) {
        super(config);
    }
    @Override
    protected Map<String, SinkConfig> initialize(Config operatorConfig) {
        Map<String, SinkConfig> sinkConfigMap = Maps.newHashMap();

        if (operatorConfig.hasPath(Constants.SINKS)) {
            Config sinks = operatorConfig.getConfig(Constants.SINKS);
            sinks.root().unwrapped().forEach((key,value) -> {
                CheckResult result = CheckConfigUtil.checkAllExists(sinks.getConfig(key),
                        SinkConfigOptions.TYPE.key(), SinkConfigOptions.PROPERTIES.key());
                if (!result.isSuccess()) {
                    throw new ConfigValidationException(CommonErrorCode.CONFIG_VALIDATION_FAILED, String.format(
                            "Sink: %s, Message: %s",
                            key, result.getMsg()));
                }

                SinkConfig sinkConfig = new JSONObject((Map<String, Object>) value).toJavaObject(SinkConfig.class);
                sinkConfig.setName(key);
                sinkConfigMap.put(key, sinkConfig);
            });

        }

        return sinkConfigMap;
    }

    @Override
    public DataStream<Event> execute(DataStream<Event> input, OperatorNode operatorNode) throws JobExecuteException {
        SinkConfig sinkConfig = operatorMap.get(operatorNode.getName());
        try {
            SinkTableFactory sinkTableFactory = FactoryUtil.discoverTableFactory(SinkTableFactory.class, sinkConfig.getType());
            Map<String, String> options = sinkConfig.getProperties();
            Configuration configuration = Configuration.fromMap(options);
            Schema schema = null;
            if(sinkConfig.getSchema() != null && !sinkConfig.getSchema().isEmpty()){
                schema = SchemaConfigParse.parseSchemaConfig(sinkConfig.getSchema());
            }
            TableFactory.Context context = new TableFactory.Context(schema, options, configuration);
            SinkProvider sinkProvider = sinkTableFactory.getSinkProvider(context);
            if(!sinkProvider.supportDynamicSchema() && schema != null && schema.isDynamic()){
                throw new UnsupportedOperationException(String.format("sink(%s) not support DynamicSchema", sinkConfig.getName()));
            }
            if(schema != null){
                System.out.println(String.format("sink(%s) dataType:\n%s", sinkConfig.getName(), schema.getDataType().toString()));
                System.out.println(String.format("sink(%s) schema:\n%s", sinkConfig.getName(), schema.getDataType().treeString()));
            }

            DataStreamSink<?> dataStreamSink = sinkProvider.consumeDataStream(input);
            if (operatorNode.getParallelism() > 0) {
                dataStreamSink.setParallelism(operatorNode.getParallelism());
            }
            dataStreamSink.name(sinkConfig.getName());
        } catch (Exception e) {
            throw new JobExecuteException("Create sink instance failed!", e);
        }

        return null;
    }

}
