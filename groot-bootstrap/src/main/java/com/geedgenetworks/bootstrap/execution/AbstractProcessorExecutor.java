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
import com.geedgenetworks.core.processor.Processor;
import com.geedgenetworks.core.processor.table.TableProcessor;
import com.geedgenetworks.core.processor.aggregate.AggregateProcessor;
import com.geedgenetworks.core.processor.projection.ProjectionProcessor;
import com.typesafe.config.Config;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

public abstract class AbstractProcessorExecutor extends AbstractExecutor<String, ProcessorConfig> {


    protected AbstractProcessorExecutor(List<URL> jarPaths, Config operatorConfig) {
        super(jarPaths, operatorConfig);
    }

    @Override
    public DataStream<Event> execute(DataStream<Event> dataStream, Node node) throws JobExecuteException {

        ProcessorConfig processorConfig = operatorMap.get(node.getName());
        boolean found = false; // 标志变量
        ServiceLoader<Processor> processors = ServiceLoader.load(Processor.class);
        for (Processor processor : processors) {
            if(processor.type().equals(processorConfig.getType())){
                found = true;
                if (node.getParallelism() > 0) {
                    processorConfig.setParallelism(node.getParallelism());
                }
                try {

                    dataStream = processor.processorFunction(
                            dataStream, processorConfig, jobRuntimeEnvironment.getStreamExecutionEnvironment().getConfig());
                } catch (Exception e) {
                    throw new JobExecuteException("Create orderby pipeline instance failed!", e);
                }
                break;
            }
        }
        if (!found) {
            throw new JobExecuteException("No matching processor found for type: " + processorConfig.getType());
        }
        return dataStream;
    }

    protected ProcessorConfig checkConfig(String key, Map<String, Object> value, Config processorsConfig) {
        ProcessorConfig projectionConfig = new ProcessorConfig();
        boolean found = false; // 标志变量
        CheckResult result = CheckConfigUtil.checkAllExists(processorsConfig.getConfig(key),
                ProjectionConfigOptions.TYPE.key());
        if (!result.isSuccess()) {
            throw new ConfigValidationException(CommonErrorCode.CONFIG_VALIDATION_FAILED, String.format(
                    "Postprocessor: %s, Message: %s",
                    key, result.getMsg()));
        }
        ServiceLoader<Processor> processors = ServiceLoader.load(Processor.class);
        for (Processor processor : processors) {
            if(processor.type().equals(value.getOrDefault("type", "").toString())){
                found = true;
                try {
                    projectionConfig = processor.checkConfig(key, value, processorsConfig);

                } catch (Exception e) {
                    throw new JobExecuteException("Create orderby pipeline instance failed!", e);
                }
                break;
            }
        }
        if (!found) {
            throw new JobExecuteException("No matching processor found for type: " + value.getOrDefault("type", "").toString());
        }
        return projectionConfig;
    }







}
