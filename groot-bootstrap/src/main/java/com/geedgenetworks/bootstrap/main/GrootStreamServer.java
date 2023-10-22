package com.geedgenetworks.bootstrap.main;

import com.alibaba.fastjson.JSONObject;
import com.geedgenetworks.common.config.GrootYamlParser;
import com.geedgenetworks.core.filter.AviatorFilter;
import com.geedgenetworks.core.pojo.*;
import com.geedgenetworks.core.processor.aggregate.AggregateProcessor;
import com.geedgenetworks.core.processor.projection.ProjectionProcessor;
import com.geedgenetworks.core.sink.Sink;
import com.geedgenetworks.core.source.Source;
import com.geedgenetworks.utils.StringUtil;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class GrootStreamServer {
    private static final Logger logger = LoggerFactory.getLogger(GrootStreamServer.class);

    public static void main(String[] args) throws Exception {

        //  conf.setString("rest.port", "9091");
        //
        //  StreamExecutionEnvironment env =
        // StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf).getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();

        Map<String, Object> yamlMap =
                GrootYamlParser.loadYaml(
                        GrootStreamServer.class
                                .getClassLoader()
                                .getResourceAsStream("groot-platform-schedule.yaml"));
        //   Map<String, Object> classMap =
        // loadYaml(Execute.class.getClassLoader().getResourceAsStream("groot-platform-plugin.yaml"));

        YamlEntity yamlEntity = new YamlEntity();
        Map<String, Object> sources = GrootYamlParser.getMap("sources", yamlMap);
        Map<String, Object> sinks = GrootYamlParser.getMap("sinks", yamlMap);
        Map<String, Object> filters = GrootYamlParser.getMap("filters", yamlMap);
        Map<String, Object> pre_processing_pipelines =
                GrootYamlParser.getMap("pre_processing_pipelines", yamlMap);
        Map<String, Object> processing_pipelines =
                GrootYamlParser.getMap("processing_pipelines", yamlMap);
        Map<String, Object> post_processing_pipelines =
                GrootYamlParser.getMap("post_processing_pipelines", yamlMap);
        Map<String, Object> application = GrootYamlParser.getMap("application", yamlMap);
        Map<String, Object> topologylist = (Map<String, Object>) application.get("topology");
        yamlEntity.setApplication(application);
        yamlEntity.setFilters(filters);
        yamlEntity.setPre_processing_pipelines(pre_processing_pipelines);
        yamlEntity.setProcessing_pipelines(processing_pipelines);
        yamlEntity.setPost_processing_pipelines(post_processing_pipelines);
        yamlEntity.setSinks(sinks);
        yamlEntity.setSources(sources);

        if (topologylist != null) {
            for (Map.Entry<String, Object> entry : topologylist.entrySet()) {
                SingleOutputStreamOperator<Event> streamlineEventSingleOutputStreamOperator = null;

                if (sources.containsKey(entry.getKey())) {
                    JSONObject jo =
                            new JSONObject((Map<String, Object>) sources.get(entry.getKey()));
                    JSONObject to = new JSONObject((Map<String, Object>) entry.getValue());
                    Topology topology = to.toJavaObject(Topology.class);
                    SourceConfig sourceConfig = jo.toJavaObject(SourceConfig.class);
                    checkSourceConfig(sourceConfig);
                    String type = sourceConfig.getType();
                    Class cls = null;
                    Source source = null;
                    try {
                        cls = Class.forName(type);
                        source = (Source) cls.newInstance();
                        sourceConfig.setName(entry.getKey());
                        if (topology.getParallelism() != 0) {

                            sourceConfig.setParallelism(topology.getParallelism());
                        }
                        streamlineEventSingleOutputStreamOperator =
                                source.source(env, sourceConfig);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    try {
                        for (String name : topology.getNext()) {
                            excute(
                                    name,
                                    streamlineEventSingleOutputStreamOperator,
                                    yamlEntity,
                                    topologylist);
                        }
                    } catch (Exception e) {
                        throw new Exception("校验未通过");
                    }
                }
            }
            env.execute((String) application.getOrDefault("name", "job"));
        } else {
            logger.error("无可执行topologylist");
            throw new Exception("无可执行topologylist");
        }
    }

    private static void excute(
            String name,
            SingleOutputStreamOperator<Event> streamlineEventSingleOutputStreamOperator,
            YamlEntity yamlEntity,
            Map<String, Object> topologyList)
            throws Exception {

        SingleOutputStreamOperator<Event> singleOutputStreamOperator =
                instantiatedTopology(
                        name, streamlineEventSingleOutputStreamOperator, yamlEntity, topologyList);
        JSONObject to = new JSONObject((Map<String, Object>) topologyList.get(name));
        Topology topology = to.toJavaObject(Topology.class);

        for (String s : topology.getNext()) {

            excute(s, singleOutputStreamOperator, yamlEntity, topologyList);
        }
    }

    private static SingleOutputStreamOperator<Event> instantiatedTopology(
            String name,
            SingleOutputStreamOperator<Event> streamlineEventSingleOutputStreamOperator,
            YamlEntity yamlEntity,
            Map<String, Object> topologyList)
            throws Exception {

        SingleOutputStreamOperator<Event> event = null;

        if (yamlEntity.getFilters() != null && yamlEntity.getFilters().containsKey(name)) {
            JSONObject jsonObject =
                    new JSONObject((Map<String, Object>) yamlEntity.getFilters().get(name));
            FilterConfig filterConfig = jsonObject.toJavaObject(FilterConfig.class);
            checkFilterConfig(filterConfig);
            String className = filterConfig.getType();
            Class cls = null;
            AviatorFilter aviatorFilter = null;
            try {
                cls = Class.forName(className);
                aviatorFilter = (AviatorFilter) cls.newInstance();
                JSONObject to = new JSONObject((Map<String, Object>) topologyList.get(name));
                Topology topology = to.toJavaObject(Topology.class);
                if (topology.getParallelism() != 0) {
                    filterConfig.setParallelism(topology.getParallelism());
                }
                filterConfig.setName(name);
                event =
                        aviatorFilter.filterFunction(
                                streamlineEventSingleOutputStreamOperator, filterConfig);

            } catch (Exception e) {
                logger.error("实例化filter:" + name + "失败");
            }

        } else if (yamlEntity.getPre_processing_pipelines() != null
                && yamlEntity.getPre_processing_pipelines().containsKey(name)) {

            JSONObject jsonObject =
                    new JSONObject(
                            (Map<String, Object>)
                                    yamlEntity.getPre_processing_pipelines().get(name));
            event =
                    InstantiateProcessor(
                            event,
                            jsonObject,
                            name,
                            streamlineEventSingleOutputStreamOperator,
                            yamlEntity,
                            topologyList);

        } else if (yamlEntity.getProcessing_pipelines() != null
                && yamlEntity.getProcessing_pipelines().containsKey(name)) {

            JSONObject jsonObject =
                    new JSONObject(
                            (Map<String, Object>) yamlEntity.getProcessing_pipelines().get(name));
            event =
                    InstantiateProcessor(
                            event,
                            jsonObject,
                            name,
                            streamlineEventSingleOutputStreamOperator,
                            yamlEntity,
                            topologyList);

        } else if (yamlEntity.getPost_processing_pipelines() != null
                && yamlEntity.getPost_processing_pipelines().containsKey(name)) {

            JSONObject jsonObject =
                    new JSONObject(
                            (Map<String, Object>)
                                    yamlEntity.getPost_processing_pipelines().get(name));
            event =
                    InstantiateProcessor(
                            event,
                            jsonObject,
                            name,
                            streamlineEventSingleOutputStreamOperator,
                            yamlEntity,
                            topologyList);

        } else if (yamlEntity.getSinks() != null && yamlEntity.getSinks().containsKey(name)) {

            JSONObject jsonObject =
                    new JSONObject((Map<String, Object>) yamlEntity.getSinks().get(name));
            JSONObject to = new JSONObject((Map<String, Object>) topologyList.get(name));
            Topology topology = to.toJavaObject(Topology.class);
            SinkConfig kafkaSinkConfig = jsonObject.toJavaObject(SinkConfig.class);
            checkSinkConfig(kafkaSinkConfig);
            String sinkclassName = kafkaSinkConfig.getType();
            Class cls = null;
            Sink sink = null;
            try {
                cls = Class.forName(sinkclassName);
                sink = (Sink) cls.newInstance();
                if (topology.getParallelism() != 0) {
                    kafkaSinkConfig.setParallelism(topology.getParallelism());
                }
                kafkaSinkConfig.setName(name);
                sink.sink(streamlineEventSingleOutputStreamOperator, kafkaSinkConfig);
            } catch (Exception e) {
                e.printStackTrace();
            }

        } else {
            logger.error("实例化sink:" + name + "失败");
        }
        return event;
    }

    private static SingleOutputStreamOperator<Event> InstantiateProcessor(
            SingleOutputStreamOperator<Event> event,
            JSONObject jsonObject,
            String name,
            SingleOutputStreamOperator<Event> streamlineEventSingleOutputStreamOperator,
            YamlEntity yamlEntity,
            Map<String, Object> topologyList)
            throws Exception {

        if (!jsonObject.containsKey("window_type")) {
            ProjectionConfig projectionConfig = jsonObject.toJavaObject(ProjectionConfig.class);

            checkProjectionConfig(projectionConfig);

            Class cls = null;
            ProjectionProcessor projectionprocessor = null;
            try {
                cls = Class.forName(projectionConfig.getType());
                projectionprocessor = (ProjectionProcessor) cls.newInstance();
                JSONObject to = new JSONObject((Map<String, Object>) topologyList.get(name));
                Topology topology = to.toJavaObject(Topology.class);
                if (topology.getParallelism() != 0) {

                    projectionConfig.setParallelism(topology.getParallelism());
                }
                projectionConfig.setName(name);
                event =
                        projectionprocessor.projectionProcessorFunction(
                                streamlineEventSingleOutputStreamOperator, projectionConfig);
            } catch (Exception e) {
                logger.error("实例化pipelines的projectionprocessor:" + name + "失败");
            }

        } else {
            AggregateConfig aggregateConfig = jsonObject.toJavaObject(AggregateConfig.class);
            checkAggregateProjectionConfig(aggregateConfig);

            Class cls = null;
            AggregateProcessor aggregateProcessor = null;
            try {
                cls = Class.forName(aggregateConfig.getType());
                aggregateProcessor = (AggregateProcessor) cls.newInstance();
                event =
                        aggregateProcessor.aggregateProcessorFunction(
                                streamlineEventSingleOutputStreamOperator, aggregateConfig);
                if (aggregateConfig.getParallelism() != 0) {
                    aggregateConfig.setParallelism(aggregateConfig.getParallelism());
                }
                aggregateConfig.setName(name);
            } catch (Exception e) {
                logger.error("实例化pipelines的aggregateProcessor:" + name + "失败");
            }
        }
        return event;
    }

    private static void checkSourceConfig(SourceConfig sourceConfig) throws Exception {

        if (!StringUtil.isNotBlank(sourceConfig.getType())) {
            throw new Exception("校验未通过 source " + sourceConfig.getName() + " type 为空！");
        }
    }

    private static void checkSinkConfig(SinkConfig sinkConfig) throws Exception {

        if (!StringUtil.isNotBlank(sinkConfig.getType())) {
            throw new Exception("校验未通过 sink " + sinkConfig.getName() + " type 为空！");
        }
    }

    private static void checkFilterConfig(FilterConfig filterConfig) throws Exception {

        if (!StringUtil.isNotBlank(filterConfig.getType())) {
            throw new Exception("校验未通过 filter " + filterConfig.getName() + " type 为空！");
        }
    }

    private static void checkProjectionConfig(ProjectionConfig projectionConfig) throws Exception {

        if (!StringUtil.isNotBlank(projectionConfig.getType())) {
            throw new Exception("校验未通过 projection " + projectionConfig.getName() + " type 为空！");
        }
    }

    private static void checkAggregateProjectionConfig(AggregateConfig aggregateConfig)
            throws Exception {

        if (!StringUtil.isNotBlank(aggregateConfig.getType())) {
            throw new Exception("校验未通过 projection " + aggregateConfig.getName() + " type 为空！");
        }
    }
}
