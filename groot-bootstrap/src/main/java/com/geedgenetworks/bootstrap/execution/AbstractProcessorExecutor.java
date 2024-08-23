package com.geedgenetworks.bootstrap.execution;

import com.alibaba.fastjson.JSONObject;
import com.geedgenetworks.bootstrap.exception.JobExecuteException;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.config.*;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.ConfigValidationException;
import com.geedgenetworks.core.pojo.AggregateConfig;
import com.geedgenetworks.core.pojo.ProcessorConfig;
import com.geedgenetworks.core.pojo.ProjectionConfig;
import com.geedgenetworks.core.pojo.TableConfig;
import com.geedgenetworks.core.processor.table.TableProcessor;
import com.geedgenetworks.core.processor.aggregate.AggregateProcessor;
import com.geedgenetworks.core.processor.projection.ProjectionProcessor;
import com.typesafe.config.Config;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.net.URL;
import java.util.List;
import java.util.Map;

public abstract class AbstractProcessorExecutor extends AbstractExecutor<String, ProcessorConfig> {


    protected AbstractProcessorExecutor(List<URL> jarPaths, Config operatorConfig) {
        super(jarPaths, operatorConfig);
    }

    @Override
    public DataStream<Event> execute(DataStream<Event> dataStream, Node node) throws JobExecuteException {

        ProcessorConfig processorConfig = operatorMap.get(node.getName());
        switch (processorConfig.getType()) {
            case "aggregate":
                dataStream = executeAggregateProcessor(dataStream, node, (AggregateConfig) processorConfig);
                break;
            case "table":
                dataStream = executeTableProcessor(dataStream, node, (TableConfig) processorConfig);
                break;
            case "projection":
                dataStream = executeProjectionProcessor(dataStream, node, (ProjectionConfig) processorConfig);
                break;
            default:// 兼容历史版本
                dataStream = executeProjectionProcessor(dataStream, node, (ProjectionConfig) processorConfig);
        }
        return dataStream;
    }
    protected DataStream<Event> executeTableProcessor(DataStream<Event> dataStream, Node node, TableConfig tableConfig) throws JobExecuteException {

        TableProcessor tableProcessor;
        if (processorMap.containsKey(tableConfig.getType())) {
            tableProcessor = (TableProcessor) processorMap.get(tableConfig.getType());
        } else {
            Class cls;
            try {
                cls = Class.forName(tableConfig.getType());
                tableProcessor = (TableProcessor) cls.newInstance();
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | RuntimeException e) {
                throw new JobExecuteException("get processing pipeline instance failed!", e);
            }
        }
        if (node.getParallelism() > 0) {
            tableConfig.setParallelism(node.getParallelism());
        }
        try {

            dataStream = tableProcessor.processorFunction(
                        dataStream, tableConfig, jobRuntimeEnvironment.getStreamExecutionEnvironment().getConfig());
        } catch (Exception e) {
            throw new JobExecuteException("Create orderby pipeline instance failed!", e);
        }
        return dataStream;
    }
    protected DataStream<Event> executeAggregateProcessor(DataStream<Event> dataStream, Node node, AggregateConfig aggregateConfig) throws JobExecuteException {

        AggregateProcessor aggregateProcessor;
        if (processorMap.containsKey(aggregateConfig.getType())) {
            aggregateProcessor = (AggregateProcessor) processorMap.get(aggregateConfig.getType());
        } else {
            Class cls;
            try {
                cls = Class.forName(aggregateConfig.getType());
                aggregateProcessor = (AggregateProcessor) cls.newInstance();
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | RuntimeException e) {
                throw new JobExecuteException("get processing pipeline instance failed!", e);
            }
        }
        if (node.getParallelism() > 0) {
            aggregateConfig.setParallelism(node.getParallelism());
        }
        try {
            dataStream =
                    aggregateProcessor.processorFunction(
                            dataStream, aggregateConfig, jobRuntimeEnvironment.getStreamExecutionEnvironment().getConfig());
        } catch (Exception e) {
            throw new JobExecuteException("Create aggregate pipeline instance failed!", e);
        }
        return dataStream;
    }

    protected DataStream<Event> executeProjectionProcessor(DataStream<Event> dataStream, Node node, ProjectionConfig projectionConfig) throws JobExecuteException {

        ProjectionProcessor projectionProcessor;
        if (processorMap.containsKey(projectionConfig.getType())) {
            projectionProcessor = (ProjectionProcessor) processorMap.get(projectionConfig.getType());
        } else {
            Class cls;
            try {
                cls = Class.forName(projectionConfig.getType());
                projectionProcessor = (ProjectionProcessor) cls.newInstance();
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | RuntimeException e) {
                throw new JobExecuteException("get processing pipeline instance failed!", e);
            }
        }
        if (node.getParallelism() > 0) {
            projectionConfig.setParallelism(node.getParallelism());
        }
        try {
            dataStream =
                    projectionProcessor.processorFunction(
                            dataStream, projectionConfig,jobRuntimeEnvironment.getStreamExecutionEnvironment().getConfig());
        } catch (Exception e) {
            throw new JobExecuteException("Create processing pipeline instance failed!", e);
        }
        return dataStream;
    }

    protected ProcessorConfig checkProcessorConfig(String key, Map<String, Object> value, Config processorsConfig) {
        ProcessorConfig projectionConfig;
        CheckResult result = CheckConfigUtil.checkAllExists(processorsConfig.getConfig(key),
                ProjectionConfigOptions.TYPE.key());
        if (!result.isSuccess()) {
            throw new ConfigValidationException(CommonErrorCode.CONFIG_VALIDATION_FAILED, String.format(
                    "Postprocessor: %s, Message: %s",
                    key, result.getMsg()));
        }
        switch ((String) value.getOrDefault("type", "")) {
            case "projection":
                projectionConfig = checkProjectionProcessorConfig(key, value, processorsConfig);
                break;
            case "aggregate":
                projectionConfig = checkAggregateProcessorConfig(key, value, processorsConfig);
                break;
            case "table":
                projectionConfig = checkTableProcessorConfig(key, value, processorsConfig);
                break;
            default://兼容历史版本
                projectionConfig = checkProjectionProcessorConfig(key, value, processorsConfig);
        }
        return projectionConfig;
    }

    protected ProcessorConfig checkProjectionProcessorConfig(String key, Map<String, Object> value, Config projectionProcessors) {

        CheckResult result = CheckConfigUtil.checkAtLeastOneExists(projectionProcessors.getConfig(key),
                ProjectionConfigOptions.OUTPUT_FIELDS.key(),
                ProjectionConfigOptions.REMOVE_FIELDS.key(),
                ProjectionConfigOptions.FUNCTIONS.key());
        if (!result.isSuccess()) {
            throw new ConfigValidationException(CommonErrorCode.CONFIG_VALIDATION_FAILED, String.format(
                    "Processor: %s, At least one of [%s] should be specified.",
                    key, String.join(",",
                            ProjectionConfigOptions.OUTPUT_FIELDS.key(),
                            ProjectionConfigOptions.REMOVE_FIELDS.key(),
                            ProjectionConfigOptions.FUNCTIONS.key())));
        }

        ProjectionConfig projectionConfig = new JSONObject(value).toJavaObject(ProjectionConfig.class);
        projectionConfig.setName(key);

        return projectionConfig;
    }


    protected AggregateConfig checkAggregateProcessorConfig(String key, Map<String, Object> value, Config aggregateProcessorsConfig) {


        CheckResult result = CheckConfigUtil.checkAllExists(aggregateProcessorsConfig.getConfig(key),
                AggregateConfigOptions.GROUP_BY_FIELDS.key(),
                AggregateConfigOptions.WINDOW_TYPE.key(),
                AggregateConfigOptions.FUNCTIONS.key(),
                AggregateConfigOptions.WINDOW_SIZE.key());
        if (!result.isSuccess()) {
            throw new ConfigValidationException(CommonErrorCode.CONFIG_VALIDATION_FAILED, String.format(
                    "Aggregate processor: %s, At least one of [%s] should be specified.",
                    key, String.join(",",
                            AggregateConfigOptions.OUTPUT_FIELDS.key(),
                            AggregateConfigOptions.REMOVE_FIELDS.key(),
                            AggregateConfigOptions.FUNCTIONS.key())));
        }

        AggregateConfig aggregateConfig = new JSONObject(value).toJavaObject(AggregateConfig.class);
        aggregateConfig.setName(key);
        return aggregateConfig;
    }

    protected TableConfig checkTableProcessorConfig(String key, Map<String, Object> value, Config config) {


        CheckResult result = CheckConfigUtil.checkAtLeastOneExists(config.getConfig(key),
                TableConfigOptions.OUTPUT_FIELDS.key(),
                TableConfigOptions.REMOVE_FIELDS.key(),
                TableConfigOptions.FUNCTIONS.key());
        if (!result.isSuccess()) {
            throw new ConfigValidationException(CommonErrorCode.CONFIG_VALIDATION_FAILED, String.format(
                    "Table processor: %s, At least one of [%s] should be specified.",
                    key, String.join(",",
                            TableConfigOptions.OUTPUT_FIELDS.key(),
                            TableConfigOptions.REMOVE_FIELDS.key(),
                            TableConfigOptions.FUNCTIONS.key())));
        }

        TableConfig tableConfig = new JSONObject(value).toJavaObject(TableConfig.class);
        tableConfig.setName(key);
        return tableConfig;
    }
}
