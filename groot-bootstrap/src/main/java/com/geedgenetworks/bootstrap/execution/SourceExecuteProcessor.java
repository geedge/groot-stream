package com.geedgenetworks.bootstrap.execution;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.geedgenetworks.bootstrap.enums.ProcessorType;
import com.geedgenetworks.bootstrap.exception.ConfigCheckException;
import com.geedgenetworks.bootstrap.exception.JobExecuteException;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.core.connector.source.SourceProvider;
import com.geedgenetworks.core.factories.FactoryUtil;
import com.geedgenetworks.core.factories.SourceTableFactory;
import com.geedgenetworks.core.factories.TableFactory;
import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.SourceConfig;
import com.geedgenetworks.core.types.Types;
import com.geedgenetworks.utils.StringUtil;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.net.URL;
import java.time.Duration;
import java.util.List;
import java.util.Map;

@Slf4j
public class SourceExecuteProcessor extends AbstractExecuteProcessor<String, SourceConfig> {
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
                if(CollectionUtils.isNotEmpty(sourceConfig.getFields())){
                    sourceConfig.setDataType(Types.parseSchemaFromJson(JSON.toJSONString(sourceConfig.getFields())));
                }
                sourceConfigMap.put(key, sourceConfig);
            });
        }

        return sourceConfigMap;
    }

    @Override
    public SingleOutputStreamOperator execute(SingleOutputStreamOperator inputDataStream, Node node) throws JobExecuteException {
        SourceConfig sourceConfig = operatorMap.get(node.getName());

        try {
            SourceTableFactory tableFactory = FactoryUtil.discoverTableFactory(SourceTableFactory.class, sourceConfig.getType());
            Map<String, String> options = sourceConfig.getProperties();
            Configuration configuration = Configuration.fromMap(options);
            TableFactory.Context context = new TableFactory.Context(sourceConfig.getDataType(), sourceConfig.getDataType(), options, configuration);
            SourceProvider sourceProvider = tableFactory.getSourceProvider(context);

            SingleOutputStreamOperator<Event> dataStream = sourceProvider.produceDataStream(jobRuntimeEnvironment.getStreamExecutionEnvironment());
            if (node.getParallelism() > 0) {
                dataStream.setParallelism(node.getParallelism());
            }
            dataStream = setWatermarkIfNecessary(dataStream, sourceConfig, node);
            return dataStream.name(sourceConfig.getName());
        } catch (Exception e) {
            throw new JobExecuteException("create source operator error", e);
        }
    }

    private SingleOutputStreamOperator<Event> setWatermarkIfNecessary(SingleOutputStreamOperator<Event> dataStream, SourceConfig sourceConfig, Node node){
        final String watermarkTimestamp = sourceConfig.getWatermark_timestamp();
        if(StringUtils.isNotBlank(watermarkTimestamp)){
            String timestampUnit = sourceConfig.getWatermark_timestamp_unit();
            Long watermarkLag = sourceConfig.getWatermark_lag();
            if(watermarkLag == null || watermarkLag < 0){
                throw new ConfigCheckException("非法的watermarkLag:" + watermarkLag);
            }
            SerializableTimestampAssigner<Event> timestampAssigner;
            if("ms".equals(timestampUnit)){
                timestampAssigner = (event, timestamp) -> {
                    Object ms = event.getExtractedFields().get(watermarkTimestamp);
                    if(ms instanceof Number){
                        return ((Number) ms).longValue();
                    }else{
                        return Long.parseLong(ms.toString());
                    }
                };
            } else if ("ms".equals(timestampUnit)) {
                timestampAssigner = (event, timestamp) -> {
                    Object s = event.getExtractedFields().get(watermarkTimestamp);
                    if(s instanceof Number){
                        return ((Number) s).longValue() * 1000;
                    }else{
                        return Long.parseLong(s.toString()) * 1000;
                    }
                };
            }else{
                throw new ConfigCheckException("不支持的timestamp_unit:" + timestampUnit);
            }

            dataStream = dataStream.assignTimestampsAndWatermarks(
                    WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofMillis(watermarkLag))
                            .withTimestampAssigner(timestampAssigner));
            if (node.getParallelism() > 0) {
                dataStream.setParallelism(node.getParallelism());
            }
        }
        return dataStream;
    }

    private void checkSourceConfig(SourceConfig sourceConfig) {
        if (!StringUtil.isNotBlank(sourceConfig.getType())) {
            throw new ConfigCheckException("校验未通过 source " + sourceConfig.getName() + " type 为空！");
        }
    }
}
