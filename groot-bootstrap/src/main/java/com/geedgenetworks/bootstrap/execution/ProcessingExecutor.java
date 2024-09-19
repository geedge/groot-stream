package com.geedgenetworks.bootstrap.execution;

import com.geedgenetworks.bootstrap.enums.ProcessorType;
import com.geedgenetworks.bootstrap.exception.JobExecuteException;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.core.pojo.ProcessorConfig;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * Initialize config and execute processor
 */
public class ProcessingExecutor extends AbstractProcessorExecutor {
    private static final String PROCESSOR_TYPE = ProcessorType.PROCESSING.getType();

    public ProcessingExecutor(List<URL> jarPaths, Config config) {
        super(jarPaths, config);
    }

    @Override
    protected Map<String, ProcessorConfig> initialize(List<URL> jarPaths, Config operatorConfig) {
        Map<String, ProcessorConfig> processingConfigMap = Maps.newHashMap();
        if (operatorConfig.hasPath(Constants.PROCESSING_PIPELINES)) {
            Config processors = operatorConfig.getConfig(Constants.PROCESSING_PIPELINES);
            processors.root().unwrapped().forEach((key, value) -> {
                processingConfigMap.put(key, checkProcessorConfig(key, (Map<String, Object>) value, processors));
            });
        }
        return processingConfigMap;
    }

    @Override
    public DataStream<Event> execute(DataStream<Event> dataStream, Node node) throws JobExecuteException {

        return super.execute(dataStream, node);
    }
}
