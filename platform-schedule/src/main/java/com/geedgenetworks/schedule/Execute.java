package com.geedgenetworks.schedule;

import com.alibaba.fastjson.JSONObject;
import com.geedgenetworks.filter.AviatorFilter;
import com.geedgenetworks.pojo.*;
import com.geedgenetworks.processor.aggregate.AggregateProcessor;
import com.geedgenetworks.processor.projection.ProjectionProcessor;
import com.geedgenetworks.sink.KafkaSink;
import com.geedgenetworks.source.Source;
import com.geedgenetworks.utils.CustomException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class Execute {
    private static final Logger logger = LoggerFactory.getLogger(Execute.class);

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
      //  conf.setString("rest.port", "9091");
    //   StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf).getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();



            Map<String, Object> sources = YamlParser.getMap("sources");
            Map<String, Object> application = YamlParser.getMap("application");
            Map<String, Object> sinks = YamlParser.getMap("sink");
            Map<String, Object> filters = YamlParser.getMap("filters");
            Map<String, Object> pre_processing_pipelines = YamlParser.getMap("pre_processing_pipelines");
            Map<String, Object> processing_pipelines = YamlParser.getMap("processing_pipelines");
            Map<String, Object> post_processing_pipelines = YamlParser.getMap("post_processing_pipelines");

            Map<String, Object> topologylist= (Map<String, Object>) application.get("topology");
            YamlEntity yamlEntity = new YamlEntity();
            yamlEntity.setApplication(application);
            yamlEntity.setFilters(filters);
            yamlEntity.setPre_processing_pipelines(pre_processing_pipelines);
            yamlEntity.setProcessing_pipelines(processing_pipelines);
            yamlEntity.setSinks(sinks);
            yamlEntity.setSources(sources);

            if (topologylist != null) {
                topologylist.forEach((key, value) -> {
                    SingleOutputStreamOperator<Event> streamlineEventSingleOutputStreamOperator = null;

                    if (sources.containsKey(key)) {
                        JSONObject jo = new JSONObject((Map<String, Object>) sources.get(key));
                        JSONObject to = new JSONObject((Map<String, Object>) value);
                        Topology topology = to.toJavaObject(Topology.class);

                        SourceConfig sourceConfig = jo.toJavaObject(SourceConfig.class);
                        String type = sourceConfig.getType();
                        Class cls = null;
                        Source source = null;
                        try {
                            cls = Class.forName(type);
                            source = (Source) cls.newInstance();
                            sourceConfig.setName(key);
                            if(topology.getParallelism()!=0 ){

                                sourceConfig.setParallelism(topology.getParallelism());
                            }
                            else{
                                sourceConfig.setParallelism((Integer) application.getOrDefault("parallelism",1));
                            }
                            streamlineEventSingleOutputStreamOperator = source.source(env, sourceConfig);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }


                        for (String name : topology.getNext()) {
                            excute(name, streamlineEventSingleOutputStreamOperator, yamlEntity,topologylist);

                        }


                    }


                    System.out.println(value);
                });
            }
            else {
                logger.error("无可执行topologylist");
                throw new CustomException("无可执行topologylist");
            }
            env.execute((String) application.getOrDefault("name","job"));







    }

    private static void excute(String name, SingleOutputStreamOperator<Event> streamlineEventSingleOutputStreamOperator, YamlEntity yamlEntity, Map<String, Object> topologyList ) {


        SingleOutputStreamOperator<Event> singleOutputStreamOperator = instantiatedTopology(name, streamlineEventSingleOutputStreamOperator,yamlEntity,topologyList);
        JSONObject to = new JSONObject((Map<String, Object>) topologyList.get(name));
        Topology topology = to.toJavaObject(Topology.class);

       for (String s :topology.getNext()) {

            excute(s, singleOutputStreamOperator, yamlEntity,topologyList);

        }
    }

    private static SingleOutputStreamOperator<Event> instantiatedTopology(String name, SingleOutputStreamOperator<Event> streamlineEventSingleOutputStreamOperator, YamlEntity yamlEntity, Map<String, Object> topologyList) {


        SingleOutputStreamOperator<Event> event = null;

        if (yamlEntity.getFilters().containsKey(name)) {
            JSONObject jsonObject = new JSONObject((Map<String, Object>) yamlEntity.getFilters().get(name));
            FilterEntity filterEntity = jsonObject.toJavaObject(FilterEntity.class);
            String className = filterEntity.getType();
            Class cls = null;
            AviatorFilter aviatorFilter = null;
            try {
                cls = Class.forName(className);
                aviatorFilter = (AviatorFilter) cls.newInstance();
                JSONObject to = new JSONObject((Map<String, Object>) topologyList.get(name));
                Topology topology = to.toJavaObject(Topology.class);
                if(topology.getParallelism()!=0 ){
                    filterEntity.setParallelism(topology.getParallelism());
                }
                else{
                    filterEntity.setParallelism((Integer) yamlEntity.getApplication().getOrDefault("parallelism",1));
                }
                filterEntity.setName(name);
                event = aviatorFilter.filterFunction(streamlineEventSingleOutputStreamOperator, filterEntity);

            } catch (Exception e) {
                logger.error("实例化filter:"+name+ "失败");
            }


        } else if (yamlEntity.getPre_processing_pipelines().containsKey(name)) {

            JSONObject jsonObject = new JSONObject((Map<String, Object>) yamlEntity.getPre_processing_pipelines().get(name));
            event=InstantiateProcessor(event,jsonObject,name, streamlineEventSingleOutputStreamOperator,yamlEntity,topologyList);

        } else if (yamlEntity.getProcessing_pipelines().containsKey(name)) {

            JSONObject jsonObject = new JSONObject((Map<String, Object>) yamlEntity.getProcessing_pipelines().get(name));
            event=InstantiateProcessor(event,jsonObject,name, streamlineEventSingleOutputStreamOperator,yamlEntity,topologyList);

        } else if (yamlEntity.getSinks().containsKey(name)) {

            JSONObject jsonObject = new JSONObject((Map<String, Object>) yamlEntity.getSinks().get(name));
            JSONObject to = new JSONObject((Map<String, Object>) topologyList.get(name));
            Topology topology = to.toJavaObject(Topology.class);

                    SinkConfig kafkaSinkConfig = jsonObject.toJavaObject(SinkConfig.class);

                    String sinkclassName = kafkaSinkConfig.getType();
                    Class cls = null;
                    KafkaSink kafkaSink = null;
                    try {
                        cls = Class.forName(sinkclassName);

                        kafkaSink = (KafkaSink) cls.newInstance();

                        if(topology.getParallelism()!=0 ){

                            kafkaSinkConfig.setParallelism(topology.getParallelism());
                        }
                        else{
                            kafkaSinkConfig.setParallelism((Integer) yamlEntity.getApplication().getOrDefault("parallelism",1));
                        }
                        kafkaSinkConfig.setName(name);
                        kafkaSink.sink(streamlineEventSingleOutputStreamOperator, kafkaSinkConfig);

                    } catch (Exception e) {
                        e.printStackTrace();
                    }

        }
        return event;
    }

    private static SingleOutputStreamOperator<Event> InstantiateProcessor(SingleOutputStreamOperator<Event> event, JSONObject jsonObject, String name, SingleOutputStreamOperator<Event> streamlineEventSingleOutputStreamOperator, YamlEntity yamlEntity, Map<String, Object> topologyList){

        if(!jsonObject.containsKey("window_type")) {
                Projection projection = jsonObject.toJavaObject(Projection.class);
                Class cls = null;
                ProjectionProcessor projectionprocessor = null;
                try {
                    cls = Class.forName(projection.getType());
                    projectionprocessor = (ProjectionProcessor) cls.newInstance();
                    JSONObject to = new JSONObject((Map<String, Object>) topologyList.get(name));
                    Topology topology = to.toJavaObject(Topology.class);
                    if (topology.getParallelism() != 0) {

                        projection.setParallelism(topology.getParallelism());
                    } else {
                        projection.setParallelism((Integer) yamlEntity.getApplication().getOrDefault("parallelism", 1));
                    }
                    projection.setName(name);
                    event = projectionprocessor.projectionProcessorFunction(streamlineEventSingleOutputStreamOperator, projection);
                } catch (Exception e) {
                    logger.error("实例化Pre_processing_pipelines的projectionprocessor:" + name + "失败");
                }

            } else {
                Aggregate aggregate = jsonObject.toJavaObject(Aggregate.class);
                Class cls = null;
                AggregateProcessor aggregateProcessor = null;
                try {
                    cls = Class.forName(aggregate.getType());
                    aggregateProcessor = (AggregateProcessor) cls.newInstance();
                    event = aggregateProcessor.aggregateProcessorFunction(streamlineEventSingleOutputStreamOperator, aggregate);
                    if (aggregate.getParallelism() != 0) {
                        aggregate.setParallelism(aggregate.getParallelism());
                    } else {
                        aggregate.setParallelism((Integer) yamlEntity.getApplication().getOrDefault("parallelism", 1));
                    }
                    aggregate.setName(name);
                } catch (Exception e) {
                    logger.error("实例化Pre_processing_pipelines的aggregateProcessor:" + name + "失败");
                }
            }
        return event;
    }
}