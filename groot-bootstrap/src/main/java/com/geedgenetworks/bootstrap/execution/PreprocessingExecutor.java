package com.geedgenetworks.bootstrap.execution;

import com.geedgenetworks.bootstrap.enums.StageType;
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
    private static final String PROCESSOR_TYPE = StageType.PREPROCESSING.getType();

    public PreprocessingExecutor(Config config) {
        super(config);
    }

    @Override
    protected Map<String, ProcessorConfig> initialize(Config operatorConfig) {
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
    public DataStream<Event> execute(DataStream<Event> input, OperatorNode operatorNode) throws JobExecuteException {

        return super.execute(input, operatorNode);

    }
}
