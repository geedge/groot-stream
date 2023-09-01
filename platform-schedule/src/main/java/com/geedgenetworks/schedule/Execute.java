package com.geedgenetworks.schedule;

import com.alibaba.fastjson.JSONObject;
import com.geedgenetworks.filter.AviatorFilter;
import com.geedgenetworks.pojo.*;
import com.geedgenetworks.processor.aggregate.AggregateProcessor;
import com.geedgenetworks.processor.projection.ProjectionProcessor;
import com.geedgenetworks.sink.KafkaSink;
import com.geedgenetworks.source.Source;
import com.geedgenetworks.utils.CustomException;
import com.geedgenetworks.utils.StringUtil;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.geedgenetworks.schedule.YamlParser.loadYaml;

public class Execute {
    private static final Logger logger = LoggerFactory.getLogger(Execute.class);

    public static void main(String[] args) throws Exception {

      //  conf.setString("rest.port", "9091");
        //
        //  StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf).getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        loadYaml(Execute.class.getClassLoader().getResourceAsStream("groot-platform-schedule.yaml"));

        YamlEntity yamlEntity = new YamlEntity();

        Map<String, Object> sources = YamlParser.getMap("sources");
        Map<String, Object> sinks = YamlParser.getMap("sinks");
        Map<String, Object> filters = YamlParser.getMap("filters");
        Map<String, Object> pre_processing_pipelines = YamlParser.getMap("pre_processing_pipelines");
        Map<String, Object> processing_pipelines = YamlParser.getMap("processing_pipelines");
        Map<String, Object> post_processing_pipelines = YamlParser.getMap("post_processing_pipelines");


            Map<String, Object> application = YamlParser.getMap("application");


            Map<String, Object> topologylist= (Map<String, Object>) application.get("topology");
            yamlEntity.setApplication(application);
           yamlEntity.setFilters(filters);
            yamlEntity.setPre_processing_pipelines(pre_processing_pipelines);
            yamlEntity.setProcessing_pipelines(processing_pipelines);
            yamlEntity.setSinks(sinks);
            yamlEntity.setSources(sources);


            if (topologylist != null) {


                for (Map.Entry<String, Object> entry : topologylist.entrySet()) {
                    SingleOutputStreamOperator<Event> streamlineEventSingleOutputStreamOperator = null;

                    if (sources.containsKey(entry.getKey())) {
                        JSONObject jo = new JSONObject((Map<String, Object>) sources.get(entry.getKey()));
                        JSONObject to = new JSONObject((Map<String, Object>) entry.getValue());
                        Topology topology = to.toJavaObject(Topology.class);

                        SourceConfig sourceConfig = jo.toJavaObject(SourceConfig.class);
                        String type = sourceConfig.getType();
                        Class cls = null;
                        Source source = null;
                        try {
                            cls = Class.forName(type);
                            source = (Source) cls.newInstance();
                            sourceConfig.setName(entry.getKey());
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


                        try {
                            for (String name : topology.getNext()) {
                                excute(name, streamlineEventSingleOutputStreamOperator, yamlEntity,topologylist);

                            }
                        }catch (Exception e){
                            throw new CustomException("校验未通过");
                        }
                    }
                }
            }
            else {
                logger.error("无可执行topologylist");
                throw new CustomException("无可执行topologylist");
            }
            env.execute((String) application.getOrDefault("name","job"));







    }



    private static void excute(String name, SingleOutputStreamOperator<Event> streamlineEventSingleOutputStreamOperator, YamlEntity yamlEntity, Map<String, Object> topologyList ) throws Exception {


        SingleOutputStreamOperator<Event> singleOutputStreamOperator = instantiatedTopology(name, streamlineEventSingleOutputStreamOperator,yamlEntity,topologyList);
        JSONObject to = new JSONObject((Map<String, Object>) topologyList.get(name));
        Topology topology = to.toJavaObject(Topology.class);

       for (String s :topology.getNext()) {

            excute(s, singleOutputStreamOperator, yamlEntity,topologyList);

        }
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
    private static void checkAggregateProjectionConfig(AggregateConfig aggregateConfig) throws Exception {


        if (!StringUtil.isNotBlank(aggregateConfig.getType())) {

            throw new Exception("校验未通过 projection " + aggregateConfig.getName() + " type 为空！");
        }

    }
    private static SingleOutputStreamOperator<Event> instantiatedTopology(String name, SingleOutputStreamOperator<Event> streamlineEventSingleOutputStreamOperator, YamlEntity yamlEntity, Map<String, Object> topologyList) throws Exception {


        SingleOutputStreamOperator<Event> event = null;

        if (yamlEntity.getFilters()!=null && yamlEntity.getFilters().containsKey(name)) {
            JSONObject jsonObject = new JSONObject((Map<String, Object>) yamlEntity.getFilters().get(name));
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
                if(topology.getParallelism()!=0 ){
                    filterConfig.setParallelism(topology.getParallelism());
                }
                else{
                    filterConfig.setParallelism((Integer) yamlEntity.getApplication().getOrDefault("parallelism",1));
                }
                filterConfig.setName(name);
                event = aviatorFilter.filterFunction(streamlineEventSingleOutputStreamOperator, filterConfig);

            } catch (Exception e) {
                logger.error("实例化filter:"+name+ "失败");
            }


        } else if (yamlEntity.getPre_processing_pipelines()!=null && yamlEntity.getPre_processing_pipelines().containsKey(name)) {

            JSONObject jsonObject = new JSONObject((Map<String, Object>) yamlEntity.getPre_processing_pipelines().get(name));
            event=InstantiateProcessor(event,jsonObject,name, streamlineEventSingleOutputStreamOperator,yamlEntity,topologyList);

        } else if (yamlEntity.getProcessing_pipelines()!=null && yamlEntity.getProcessing_pipelines().containsKey(name)) {

            JSONObject jsonObject = new JSONObject((Map<String, Object>) yamlEntity.getProcessing_pipelines().get(name));
            event=InstantiateProcessor(event,jsonObject,name, streamlineEventSingleOutputStreamOperator,yamlEntity,topologyList);
        } else if (yamlEntity.getPost_processing_pipelines()!=null && yamlEntity.getPost_processing_pipelines().containsKey(name)) {

            JSONObject jsonObject = new JSONObject((Map<String, Object>) yamlEntity.getPost_processing_pipelines().get(name));
            event=InstantiateProcessor(event,jsonObject,name, streamlineEventSingleOutputStreamOperator,yamlEntity,topologyList);

        } else if (yamlEntity.getSinks()!=null && yamlEntity.getSinks().containsKey(name)) {

            JSONObject jsonObject = new JSONObject((Map<String, Object>) yamlEntity.getSinks().get(name));
            JSONObject to = new JSONObject((Map<String, Object>) topologyList.get(name));
            Topology topology = to.toJavaObject(Topology.class);

            SinkConfig kafkaSinkConfig = jsonObject.toJavaObject(SinkConfig.class);
            checkSinkConfig(kafkaSinkConfig);

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
        else{

            logger.error("实例化filter:"+name+ "失败");


        }
        return event;
    }

    private static SingleOutputStreamOperator<Event> InstantiateProcessor(SingleOutputStreamOperator<Event> event, JSONObject jsonObject, String name, SingleOutputStreamOperator<Event> streamlineEventSingleOutputStreamOperator, YamlEntity yamlEntity, Map<String, Object> topologyList) throws Exception {

        if(!jsonObject.containsKey("window_type")) {
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
                    } else {
                        projectionConfig.setParallelism((Integer) yamlEntity.getApplication().getOrDefault("parallelism", 1));
                    }
                    projectionConfig.setName(name);
                    event = projectionprocessor.projectionProcessorFunction(streamlineEventSingleOutputStreamOperator, projectionConfig);
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
                    event = aggregateProcessor.aggregateProcessorFunction(streamlineEventSingleOutputStreamOperator, aggregateConfig);
                    if (aggregateConfig.getParallelism() != 0) {
                        aggregateConfig.setParallelism(aggregateConfig.getParallelism());
                    } else {
                        aggregateConfig.setParallelism((Integer) yamlEntity.getApplication().getOrDefault("parallelism", 1));
                    }
                    aggregateConfig.setName(name);
                } catch (Exception e) {
                    logger.error("实例化pipelines的aggregateProcessor:" + name + "失败");
                }
            }
        return event;
    }
}