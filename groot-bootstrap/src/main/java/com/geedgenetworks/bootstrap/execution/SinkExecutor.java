package com.geedgenetworks.bootstrap.execution;

import com.alibaba.fastjson.JSONObject;
import com.geedgenetworks.bootstrap.enums.JobStage;
import com.geedgenetworks.bootstrap.exception.JobExecuteException;
import com.geedgenetworks.bootstrap.utils.SchemaConfigParse;
import com.geedgenetworks.common.config.Constants;
import com.geedgenetworks.common.config.CheckConfigUtil;
import com.geedgenetworks.common.config.CheckResult;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.ConfigValidationException;
import com.geedgenetworks.api.connector.sink.SinkConfig;
import com.geedgenetworks.api.connector.sink.SinkConfigOptions;
import com.geedgenetworks.api.connector.sink.SinkProvider;
import com.geedgenetworks.api.factory.SinkFactory;
import com.geedgenetworks.api.event.Event;
import com.geedgenetworks.api.factory.FactoryUtil;
import com.geedgenetworks.api.factory.ConnectorFactory;
import com.geedgenetworks.api.connector.schema.Schema;
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
public class SinkExecutor extends AbstractExecutor<JobRuntimeEnvironment, Config> {
    private static final String PROCESSOR_TYPE = JobStage.SINK.getType();
    private Map<String, SinkConfig> operators;
    public SinkExecutor(JobRuntimeEnvironment environment, Config jobConfig) {
        super(environment, jobConfig);
    }
    @Override
    protected void initialize(Config jobConfig) {
        operators = Maps.newHashMap();
        if (jobConfig.hasPath(Constants.SINKS)) {
            Config sinks = jobConfig.getConfig(Constants.SINKS);
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
                operators.put(key, sinkConfig);
            });

        }

    }

    @Override
    public DataStream<Event> execute(DataStream<Event> input, JobTopologyNode jobTopologyNode) throws JobExecuteException {
        SinkConfig sinkConfig = operators.get(jobTopologyNode.getName());
        try {
            SinkFactory sinkTableFactory = FactoryUtil.discoverConnectorFactory(SinkFactory.class, sinkConfig.getType());
            Map<String, String> options = sinkConfig.getProperties();
            Configuration configuration = Configuration.fromMap(options);
            Schema schema = null;
            if(sinkConfig.getSchema() != null && !sinkConfig.getSchema().isEmpty()){
                schema = SchemaConfigParse.parseSchemaConfig(sinkConfig.getSchema());
            }
            ConnectorFactory.Context context = new ConnectorFactory.Context(schema, options, configuration);
            SinkProvider sinkProvider = sinkTableFactory.getSinkProvider(context);
            if(!sinkProvider.supportDynamicSchema() && schema != null && schema.isDynamic()){
                throw new UnsupportedOperationException(String.format("sink(%s) not support DynamicSchema", sinkConfig.getName()));
            }
            if(schema != null){
                System.out.println(String.format("sink(%s) dataType:\n%s", sinkConfig.getName(), schema.getDataType().toString()));
                System.out.println(String.format("sink(%s) schema:\n%s", sinkConfig.getName(), schema.getDataType().treeString()));
            }

            DataStreamSink<?> dataStreamSink = sinkProvider.consumeDataStream(input);
            if (jobTopologyNode.getParallelism() > 0) {
                dataStreamSink.setParallelism(jobTopologyNode.getParallelism());
            }
            dataStreamSink.name(sinkConfig.getName());
        } catch (Exception e) {
            throw new JobExecuteException("Create sink instance failed!", e);
        }

        return null;
    }

}
