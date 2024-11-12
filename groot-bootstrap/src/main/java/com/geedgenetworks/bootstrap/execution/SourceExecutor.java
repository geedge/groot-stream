package com.geedgenetworks.bootstrap.execution;

import com.alibaba.fastjson2.JSONObject;
import com.geedgenetworks.bootstrap.enums.OperatorType;
import com.geedgenetworks.bootstrap.exception.ConfigCheckException;
import com.geedgenetworks.bootstrap.exception.JobExecuteException;
import com.geedgenetworks.bootstrap.utils.SchemaConfigParse;
import com.geedgenetworks.common.config.Constants;
import com.geedgenetworks.common.config.CheckConfigUtil;
import com.geedgenetworks.common.config.CheckResult;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.ConfigValidationException;
import com.geedgenetworks.api.connector.source.SourceConfigOptions;
import com.geedgenetworks.api.connector.source.SourceConfig;
import com.geedgenetworks.api.connector.source.SourceProvider;
import com.geedgenetworks.api.connector.source.SourceTableFactory;
import com.geedgenetworks.api.connector.event.Event;
import com.geedgenetworks.api.factory.FactoryUtil;
import com.geedgenetworks.api.factory.ConnectorFactory;
import com.geedgenetworks.api.connector.schema.Schema;
import com.google.common.collect.Maps;
import com.typesafe.config.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.time.Duration;
import java.util.Map;

/**
 * Initialize config and execute source connector
 */
@Slf4j
public class SourceExecutor extends AbstractExecutor<JobRuntimeEnvironment, Config> {
    private static final String PROCESSOR_TYPE = OperatorType.SOURCE.getType();
    private Map<String, SourceConfig> operators;
    public SourceExecutor(JobRuntimeEnvironment environment, Config jobConfig) {
        super(environment, jobConfig);
    }
    @Override
    protected void initialize(Config jobConfig) {
        operators = Maps.newHashMap();
        if (jobConfig.hasPath(Constants.SOURCES)) {
            Config sources = jobConfig.getConfig(Constants.SOURCES);
            sources.root().unwrapped().forEach((key,value) -> {
               CheckResult result = CheckConfigUtil.checkAllExists(sources.getConfig(key),
                        SourceConfigOptions.TYPE.key(), SourceConfigOptions.PROPERTIES.key());
                if (!result.isSuccess()) {
                    throw new ConfigValidationException(CommonErrorCode.CONFIG_VALIDATION_FAILED, String.format(
                            "Source: %s, Message: %s",
                            key, result.getMsg()));
                }

                SourceConfig sourceConfig = new JSONObject((Map<String, Object>) value).toJavaObject(SourceConfig.class);
                sourceConfig.setName(key);
                operators.put(key, sourceConfig);
            });

        }
    }

    @Override
    public DataStream<Event> execute(DataStream<Event> input, JobTopologyNode jobTopologyNode) throws JobExecuteException {
        SourceConfig sourceConfig = operators.get(jobTopologyNode.getName());
        SingleOutputStreamOperator sourceSingleOutputStreamOperator;
        try {
            SourceTableFactory tableFactory = FactoryUtil.discoverConnectorFactory(SourceTableFactory.class, sourceConfig.getType());
            Map<String, String> options = sourceConfig.getProperties();
            Configuration configuration = Configuration.fromMap(options);
            Schema schema = null;
            if(sourceConfig.getSchema() != null && !sourceConfig.getSchema().isEmpty()){
                schema = SchemaConfigParse.parseSchemaConfig(sourceConfig.getSchema());
            }
            ConnectorFactory.Context context = new ConnectorFactory.Context(schema, options, configuration);
            SourceProvider sourceProvider = tableFactory.getSourceProvider(context);
            if(!sourceProvider.supportDynamicSchema() && schema != null && schema.isDynamic()){
                throw new UnsupportedOperationException(String.format("source(%s) not support DynamicSchema", sourceConfig.getName()));
            }
            if(schema != null){
                System.out.println(String.format("source(%s) dataType:\n%s", sourceConfig.getName(), schema.getDataType().toString()));
                System.out.println(String.format("source(%s) schema:\n%s", sourceConfig.getName(), schema.getDataType().treeString()));
            }

            sourceSingleOutputStreamOperator = sourceProvider.produceDataStream(environment.getStreamExecutionEnvironment()).name(sourceConfig.getName());
            if (jobTopologyNode.getParallelism() > 0) {
                sourceSingleOutputStreamOperator.setParallelism(jobTopologyNode.getParallelism());
            }
            sourceSingleOutputStreamOperator = setWatermarkIfNecessary(sourceSingleOutputStreamOperator, sourceConfig, jobTopologyNode);
            return sourceSingleOutputStreamOperator;
        } catch (Exception e) {
            throw new JobExecuteException("Create source instance failed!", e);
        }
    }

    private SingleOutputStreamOperator<Event> setWatermarkIfNecessary(SingleOutputStreamOperator<Event> dataStream, SourceConfig sourceConfig, JobTopologyNode jobTopologyNode){
        final String watermarkTimestamp = sourceConfig.getWatermark_timestamp();
        if(StringUtils.isNotBlank(watermarkTimestamp)){
            String timestampUnit = sourceConfig.getWatermark_timestamp_unit();
            Long watermarkLag = sourceConfig.getWatermark_lag();
            if (watermarkLag == null || watermarkLag < 0) {
                throw new ConfigCheckException("非法的watermarkLag:" + watermarkLag);
            }
            SerializableTimestampAssigner<Event> timestampAssigner;
            if ("ms".equalsIgnoreCase(timestampUnit)) {
                timestampAssigner = (event, timestamp) -> {
                    Object ms = event.getExtractedFields().get(watermarkTimestamp);
                    if (ms == null) {
                        return 0;
                    } else if (ms instanceof Number) {
                        return ((Number) ms).longValue();
                    } else{
                        return str2Long(ms.toString());
                    }
                };
            } else if ("s".equalsIgnoreCase(timestampUnit)) {
                timestampAssigner = (event, timestamp) -> {
                    Object s = event.getExtractedFields().get(watermarkTimestamp);
                    if (s == null) {
                        return 0;
                    } else if (s instanceof Number) {
                        return ((Number) s).longValue() * 1000;
                    } else {
                        return str2Long(s.toString()) * 1000;
                    }
                };
            } else {
                throw new ConfigCheckException("不支持的timestamp_unit:" + timestampUnit);
            }

            dataStream = dataStream.assignTimestampsAndWatermarks(
                    WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofMillis(watermarkLag))
                            .withTimestampAssigner(timestampAssigner)
            );
            if (jobTopologyNode.getParallelism() > 0) {
                dataStream.setParallelism(jobTopologyNode.getParallelism());
            }
        }
        return dataStream;
    }

    private static long str2Long(String str){
        try {
            return Long.parseLong(str);
        } catch (NumberFormatException e) {
            return 0;
        }
    }

}
