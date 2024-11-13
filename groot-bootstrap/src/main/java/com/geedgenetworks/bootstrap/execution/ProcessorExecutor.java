package com.geedgenetworks.bootstrap.execution;

import com.geedgenetworks.api.processor.ProcessorConfigOptions;
import com.geedgenetworks.api.factory.FactoryUtil;
import com.geedgenetworks.api.factory.ProcessorFactory;
import com.geedgenetworks.bootstrap.exception.JobExecuteException;
import com.geedgenetworks.common.config.CheckConfigUtil;
import com.geedgenetworks.common.config.CheckResult;
import com.geedgenetworks.common.config.Constants;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.ConfigValidationException;
import com.geedgenetworks.api.processor.Processor;
import com.geedgenetworks.api.processor.ProcessorConfig;
import com.geedgenetworks.api.event.Event;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import org.apache.flink.streaming.api.datastream.DataStream;
import java.util.Map;
/**
 * Initialize config and execute processor
 */
public class ProcessorExecutor extends AbstractExecutor<JobRuntimeEnvironment, Config> {
    private Map<String, ProcessorConfig> operators;
    private Map<String, Processor<?>> processors;

    public ProcessorExecutor(JobRuntimeEnvironment environment, Config jobConfig) {
        super(environment, jobConfig);
    }

    @Override
    protected void initialize(Config jobConfig) {
        operators = Maps.newHashMap();
        processors = Maps.newHashMap();

        if (jobConfig.hasPath(Constants.FILTERS)) {
            discoveryProcessors(jobConfig.getConfig(Constants.FILTERS));
        }

        if (jobConfig.hasPath(Constants.SPLITS)) {
            discoveryProcessors(jobConfig.getConfig(Constants.SPLITS));
        }

        if (jobConfig.hasPath(Constants.PREPROCESSING_PIPELINES)) {
            discoveryProcessors(jobConfig.getConfig(Constants.PREPROCESSING_PIPELINES));
        }

        if (jobConfig.hasPath(Constants.PROCESSING_PIPELINES)) {
            discoveryProcessors(jobConfig.getConfig(Constants.PROCESSING_PIPELINES));
        }

        if (jobConfig.hasPath(Constants.POSTPROCESSING_PIPELINES)) {
            discoveryProcessors(jobConfig.getConfig(Constants.POSTPROCESSING_PIPELINES));
        }
    }

    private void discoveryProcessors(Config config) {

        config.root().unwrapped().forEach((key, value) -> {

            CheckResult result = CheckConfigUtil.checkAllExists(config.getConfig(key),
                    ProcessorConfigOptions.TYPE.key());
            if (!result.isSuccess()) {
                throw new ConfigValidationException(CommonErrorCode.CONFIG_VALIDATION_FAILED, String.format(
                        "Processor: %s, Message: %s", key, result.getMsg()));
            }

            Processor processor = FactoryUtil
                    .discoverProcessorFactory(ProcessorFactory.class, ((Map<?, ?>) value).get("type").toString()).createProcessor();
            processors.put(key, processor);
            operators.put(key, processor.parseConfig(key,config.getConfig(key)));
        });

    }

    @Override
    public DataStream<Event> execute(DataStream<Event> input, JobTopologyNode jobTopologyNode) throws JobExecuteException {
        String name = jobTopologyNode.getName();
        ProcessorConfig operatorConfig = operators.get(name);
        if (operatorConfig == null) {
            throw new JobExecuteException("No matching operator configuration found for: " + name);
        }

        Processor processor = processors.get(operatorConfig.getName());

        if (processor == null) {
            throw new JobExecuteException("No matching processor found for type: " + operatorConfig.getType());
        }

        // Set parallelism if needed
        int parallelism = jobTopologyNode.getParallelism();
        if (parallelism > 0) {
            operatorConfig.setParallelism(parallelism);
        }

        try {
           return processor.process(environment.getStreamExecutionEnvironment(), input, operatorConfig);
        } catch (Exception e) {
            throw new JobExecuteException("Failed to execute processor due to unexpected error.", e);
        }

    }
}
