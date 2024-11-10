package com.geedgenetworks.bootstrap.execution;

import com.geedgenetworks.bootstrap.enums.StageType;
import com.geedgenetworks.bootstrap.exception.JobExecuteException;
import com.geedgenetworks.common.config.Constants;
import com.geedgenetworks.spi.processor.Processor;
import com.geedgenetworks.spi.processor.ProcessorConfig;
import com.geedgenetworks.spi.processor.ProcessorProvider;
import com.geedgenetworks.spi.table.event.Event;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Initialize config and execute processor
 */
public class ProcessingExecutor extends AbstractProcessorExecutor {
    private static final String PROCESSOR_TYPE = StageType.PROCESSING.getType();
    //private Map<String, Processor<?>> processors;

    public ProcessingExecutor(Config config) {
        super(config);
    }

    @Override
    protected Map<String, ProcessorConfig> initialize(Config operatorConfig) {
        Map<String, ProcessorConfig> processingConfigMap = Maps.newHashMap();
        //processors = new HashMap<>();
        if (operatorConfig.hasPath(Constants.PROCESSING_PIPELINES)) {
            Config processingConfig = operatorConfig.getConfig(Constants.PROCESSING_PIPELINES);
            processingConfig.root().unwrapped().forEach((key, value) -> {
                processingConfigMap.put(key, checkConfig(key, (Map<String, Object>) value, processingConfig));
                //processors.put(key, ProcessorProvider.load(((Map<?, ?>) value).get("type").toString()));

            });
        }
        return processingConfigMap;
    }

    @Override
    public DataStream<Event> execute(DataStream<Event> input, OperatorNode operatorNode) throws JobExecuteException {
        return super.execute(input, operatorNode);
    }
}
