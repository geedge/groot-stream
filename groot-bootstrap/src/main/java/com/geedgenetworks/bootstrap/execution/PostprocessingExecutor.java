package com.geedgenetworks.bootstrap.execution;

import com.geedgenetworks.bootstrap.enums.StageType;
import com.geedgenetworks.bootstrap.exception.JobExecuteException;
import com.geedgenetworks.common.config.Constants;
import com.geedgenetworks.spi.processor.ProcessorConfig;
import com.geedgenetworks.spi.table.event.Event;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * Initialize config and execute postprocessor
 */
public class PostprocessingExecutor extends AbstractProcessorExecutor {
    private static final String PROCESSOR_TYPE = StageType.POSTPROCESSING.getType();

    public PostprocessingExecutor(Config config) {
        super(config);
    }

    @Override
    protected Map<String, ProcessorConfig> initialize(Config operatorConfig) {
        Map<String, ProcessorConfig> postprocessingConfigMap = Maps.newHashMap();
        if (operatorConfig.hasPath(Constants.POSTPROCESSING_PIPELINES)) {
            Config postprocessors = operatorConfig.getConfig(Constants.POSTPROCESSING_PIPELINES);
            postprocessors.root().unwrapped().forEach((key, value) -> {
                postprocessingConfigMap.put(key, checkConfig(key, (Map<String, Object>) value, postprocessors));
            });
        }
        return postprocessingConfigMap;
    }


    @Override
    public DataStream<Event> execute(DataStream<Event> input, OperatorNode operatorNode) throws JobExecuteException {
        return super.execute(input, operatorNode);
    }
}
