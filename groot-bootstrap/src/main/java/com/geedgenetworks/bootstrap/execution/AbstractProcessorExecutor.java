package com.geedgenetworks.bootstrap.execution;

import com.geedgenetworks.bootstrap.exception.JobExecuteException;
import com.geedgenetworks.common.config.*;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.ConfigValidationException;
import com.geedgenetworks.spi.configuration.ProjectionConfigOptions;
import com.geedgenetworks.spi.processor.Processor;
import com.geedgenetworks.spi.processor.ProcessorConfig;
import com.geedgenetworks.spi.table.event.Event;
import com.typesafe.config.Config;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

public abstract class AbstractProcessorExecutor extends AbstractExecutor<String, ProcessorConfig> {


    protected AbstractProcessorExecutor(Config operatorConfig) {
        super(operatorConfig);
    }

    @Override
    public DataStream<Event> execute(DataStream<Event> input, OperatorNode operatorNode) throws JobExecuteException {

        ProcessorConfig processorConfig = operatorMap.get(operatorNode.getName());
        boolean found = false; // 标志变量
        ServiceLoader<Processor> processors = ServiceLoader.load(Processor.class);
        for (Processor processor : processors) {
            if(processor.type().equals(processorConfig.getType())){
                found = true;
                if (operatorNode.getParallelism() > 0) {
                    processorConfig.setParallelism(operatorNode.getParallelism());
                }
                try {
                    input = processor.process(jobRuntimeEnvironment.getStreamExecutionEnvironment(), input, processorConfig);
                } catch (Exception e) {
                    throw new JobExecuteException("Create orderby pipeline instance failed!", e);
                }
                break;
            }
        }
        if (!found) {
            throw new JobExecuteException("No matching processor found for type: " + processorConfig.getType());
        }
        return input;
    }

    protected ProcessorConfig checkConfig(String key, Map<String, Object> value, Config processorsConfig) {
        ProcessorConfig ProcessorConfig = new ProcessorConfig();
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
                    ProcessorConfig = processor.checkConfig(key, value, processorsConfig);

                } catch (Exception e) {
                    throw new JobExecuteException("Create orderby pipeline instance failed!", e);
                }
                break;
            }
        }
        if (!found) {
            throw new JobExecuteException("No matching processor found for type: " + value.getOrDefault("type", "").toString());
        }
        return ProcessorConfig;
    }







}
