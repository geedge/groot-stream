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
 * Initialize config and execute postprocessor
 */
public class PostprocessingExecutor extends AbstractProcessorExecutor {
    private static final String PROCESSOR_TYPE = ProcessorType.POSTPROCESSING.getType();

    public PostprocessingExecutor(List<URL> jarPaths, Config config) {
        super(jarPaths, config);
    }

    @Override
    protected Map<String, ProcessorConfig> initialize(List<URL> jarPaths, Config operatorConfig) {
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
    public DataStream<Event> execute(DataStream<Event> dataStream, Node node) throws JobExecuteException {
        return super.execute(dataStream, node);
    }
}
