package com.geedgenetworks.bootstrap.execution;

import com.geedgenetworks.bootstrap.enums.ProcessorType;
import com.geedgenetworks.bootstrap.exception.JobExecuteException;
import com.geedgenetworks.common.config.Constants;
import com.geedgenetworks.spi.processor.ProcessorConfig;
import com.geedgenetworks.spi.table.event.Event;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * Initialize config and execute preprocessor
 */
@Slf4j
public class PreprocessingExecutor extends AbstractProcessorExecutor {
    private static final String PROCESSOR_TYPE = ProcessorType.PREPROCESSING.getType();

    public PreprocessingExecutor(List<URL> jarPaths, Config config) {
        super(jarPaths, config);
    }

    @Override
    protected Map<String, ProcessorConfig> initialize(List<URL> jarPaths, Config operatorConfig) {
        Map<String, ProcessorConfig> preprocessingConfigMap = Maps.newHashMap();
        if (operatorConfig.hasPath(Constants.PREPROCESSING_PIPELINES)) {
            Config preprocessors = operatorConfig.getConfig(Constants.PREPROCESSING_PIPELINES);
            preprocessors.root().unwrapped().forEach((key, value) -> {
                preprocessingConfigMap.put(key, checkConfig(key, (Map<String, Object>) value, preprocessors));
            });
        }
        return preprocessingConfigMap;
    }

    @Override
    public DataStream<Event> execute(DataStream<Event> dataStream, Node node) throws JobExecuteException {

        return super.execute(dataStream, node);

    }
}
