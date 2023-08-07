package net.geedgenetworks.schedule;

import com.alibaba.fastjson.JSONObject;
import net.geedgenetworks.filter.AviatorFilter;
import net.geedgenetworks.pojo.*;
import net.geedgenetworks.processor.aggregate.AggregateProcessor;
import net.geedgenetworks.processor.projection.ProjectionProcessor;
import net.geedgenetworks.sink.KafkaSink;
import net.geedgenetworks.source.Source;
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
     //  StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf).getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

 /*       String hostname1 = "192.168.44.12";
        int port1 = 9995;

        String hostname2 = "192.168.44.12";
        int port2 = 9994;

        // Create DataStreams from the socket sources
        DataStream<String> stream1 = env.addSource(new SocketTextStreamFunction(hostname1, port1, "\n", -1));
        DataStream<String> stream2 = env.addSource(new SocketTextStreamFunction(hostname2, port2, "\n", -1));

        // Merge the two streams into a single stream (if needed)
        DataStream<String> mergedStream = stream1.union(stream2);

        // Process the data as needed (e.g., printing to the console)
        mergedStream.print();*/


       // env.addSource(new UdpClientSource("192.168.58.56", 9999)).print();


/*

        DataStream<String> udpStream1 = env.addSource(new CustomUDPSource(9994));
        DataStream<String> udpStream2 = env.addSource(new CustomUDPSource(9995));
        DataStream<String> mergedStream = udpStream1.union(udpStream2);
*/

        // Process the data stream as needed (e.g., print the received data)





        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // sourcetest schedule = (sourcetest) cls.newInstance();


        // 指定YAML文件的路径



   /*     String yamlFilePath = Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource("business.yaml")).getPath();



        Yaml yaml = new Yaml();
*/

/*        URL url = new URL("http://192.168.40.223:8186/hos/test5/yaml");
        //打开链接
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        //设置请求方式为"GET"
        conn.setRequestMethod("GET");
        //超时响应时间为5秒
        conn.setConnectTimeout(5 * 1000);*/
        //通过输入流获取图片数据
  /*      Map<String, Object> yamlData = null;
        try {
           // InputStream inputStream = conn.getInputStream();
            InputStream inputStream = Execute.class
                    .getClassLoader()
                    .getResourceAsStream("business.yaml");
            //InputStream inputStream = new FileInputStream(yamlFilePath);

          //  InputStream inputStream = Execute.class.getResourceAsStream("/home/tsg/olap/topology/test/business.yaml");

            yamlData = yaml.load(inputStream);

            System.out.println("YAML文件内容: " + yamlData);
        } catch (Exception e) {
            System.err.println("YAML文件未找到");
        }*/
/*
        LinkedHashMap<String, Object> source = (LinkedHashMap<String, Object>) yamlData.get("Sources");

        LinkedHashMap<String, Object> filter = (LinkedHashMap<String, Object>) yamlData.get("Filters");

        LinkedHashMap<String, Object> pre_processor = (LinkedHashMap<String, Object>) yamlData.get("Pre_processing_Pipelines");
        LinkedHashMap<String, Object> projection_processor = (LinkedHashMap<String, Object>) yamlData.get("Processing_Pipelines");
        LinkedHashMap<String, Object> sink = (LinkedHashMap<String, Object>) yamlData.get("Sinks");

        Map<String, Object> excuter = (Map<String, Object>) yamlData.get("excuter");


        LinkedHashMap<String, ArrayList<String>> list = (LinkedHashMap<String, ArrayList<String>>) excuter.get("Sources");

        LinkedHashMap<String, ArrayList<String>> pip = (LinkedHashMap<String, ArrayList<String>>) excuter.get("Operators");

*/
      Map<String, Object> sources = YamlParser.getMap("sources");
        Map<String, Object> application = YamlParser.getMap("application");
        Map<String, Object> sinks = YamlParser.getMap("sink");
        Map<String, Object> filters = YamlParser.getMap("filters");
        Map<String, Object> pre_processing_pipelines = YamlParser.getMap("pre_processing_pipelines");
        Map<String, Object> processing_pipelines = YamlParser.getMap("processing_pipelines");
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
                SingleOutputStreamOperator<GrootEvent> streamlineEventSingleOutputStreamOperator = null;

                if (sources.containsKey(key)) {
                    JSONObject jo = new JSONObject((Map<String, Object>) sources.get(key));
                //    Topology topology = (Topology) value;
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
                            } catch (InstantiationException e) {
                                e.printStackTrace();
                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                            } catch (ClassNotFoundException e) {
                                e.printStackTrace();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }


                    for (String name : topology.getNext()) {
                        excute(name, streamlineEventSingleOutputStreamOperator, yamlEntity,topologylist);

                    }


                } else {


                }


                System.out.println(value);
            });
        }


        env.execute((String) application.getOrDefault("name","job"));


    }

    private static void excute(String name, SingleOutputStreamOperator<GrootEvent> streamlineEventSingleOutputStreamOperator, YamlEntity yamlEntity,Map<String, Object> topologyList ) {


        SingleOutputStreamOperator<GrootEvent> singleOutputStreamOperator = test(name, streamlineEventSingleOutputStreamOperator,yamlEntity,topologyList);

        JSONObject to = new JSONObject((Map<String, Object>) topologyList.get(name));
        Topology topology = to.toJavaObject(Topology.class);

       for (String s :topology.getNext()) {

            excute(s, singleOutputStreamOperator, yamlEntity,topologyList);

        }
    }

    private static SingleOutputStreamOperator<GrootEvent> test(String name, SingleOutputStreamOperator<GrootEvent> streamlineEventSingleOutputStreamOperator,YamlEntity yamlEntity,Map<String, Object> topologyList) {


        SingleOutputStreamOperator<GrootEvent> event = null;

        if (yamlEntity.getFilters().containsKey(name)) {
            JSONObject jsonObject = new JSONObject((Map<String, Object>) yamlEntity.getFilters().get(name));
            FilterEntity filterEntity = jsonObject.toJavaObject(FilterEntity.class);
           // List<com.zdjizhi.base.common.regular_rule> listRule = f.getJSONArray("rule_list").toJavaList(com.zdjizhi.base.common.regular_rule.class);

            String className = filterEntity.getType();
            Class cls = null;
            AviatorFilter filterImpl = null;
            try {
                cls = Class.forName(className);
                filterImpl = (AviatorFilter) cls.newInstance();

                JSONObject to = new JSONObject((Map<String, Object>) topologyList.get(name));
                Topology topology = to.toJavaObject(Topology.class);
                if(topology.getParallelism()!=0 ){

                    filterEntity.setParallelism(topology.getParallelism());
                }
                else{
                    filterEntity.setParallelism((Integer) yamlEntity.getApplication().getOrDefault("parallelism",1));
                }
                filterEntity.setName(name);
                event = filterImpl.filterFunction(streamlineEventSingleOutputStreamOperator, filterEntity);

            } catch (Exception e) {
                e.printStackTrace();
            }


        } else if (yamlEntity.getPre_processing_pipelines().containsKey(name)) {
            JSONObject jsonObject = new JSONObject((Map<String, Object>) yamlEntity.getPre_processing_pipelines().get(name));

            if(!jsonObject.containsKey("window_type")){


                //   List<com.zdjizhi.base.common.projection_function> functionlist = ppr.getJSONArray("functionlist").toJavaList(com.zdjizhi.base.common.projection_function.class);


                Projection projection = jsonObject.toJavaObject(Projection.class) ;
                Class cls = null;

                ProjectionProcessor projectionprocessor = null;
                try {
                    cls = Class.forName(projection.getType());
                    projectionprocessor = (ProjectionProcessor) cls.newInstance();
                    JSONObject to = new JSONObject((Map<String, Object>) topologyList.get(name));
                    Topology topology = to.toJavaObject(Topology.class);
                    if(topology.getParallelism()!=0 ){

                        projection.setParallelism(topology.getParallelism());
                    }
                    else{
                        projection.setParallelism((Integer) yamlEntity.getApplication().getOrDefault("parallelism",1));
                    }
                    projection.setName(name);


                    event = projectionprocessor.projectionProcessorFunction(streamlineEventSingleOutputStreamOperator, projection);

                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
            else{


                //    List<com.zdjizhi.base.common.aggregate_function> functionlist = ppr.getJSONArray("functionlist").toJavaList(com.zdjizhi.base.common.aggregate_function.class);
                Aggregate aggregate = jsonObject.toJavaObject(Aggregate.class) ;
                Class cls = null;

                AggregateProcessor aggregateProcessor = null;
                try {
                    cls = Class.forName(aggregate.getType());

                    aggregateProcessor = (AggregateProcessor) cls.newInstance();
                    event = aggregateProcessor.aggregateProcessorFunction(streamlineEventSingleOutputStreamOperator, aggregate);

                    if(aggregate.getParallelism()!=0 ){

                        aggregate.setParallelism(aggregate.getParallelism());
                    }
                    else{
                        aggregate.setParallelism((Integer) yamlEntity.getApplication().getOrDefault("parallelism",1));
                    }
                    aggregate.setName(name);

                } catch (Exception e) {
                    e.printStackTrace();
                }




            }


        } else if (yamlEntity.getProcessing_pipelines().containsKey(name)) {

            JSONObject jsonObject = new JSONObject((Map<String, Object>) yamlEntity.getProcessing_pipelines().get(name));
            if(!jsonObject.containsKey("window_type")){


                //   List<com.zdjizhi.base.common.projection_function> functionlist = ppr.getJSONArray("functionlist").toJavaList(com.zdjizhi.base.common.projection_function.class);


                Projection projection = jsonObject.toJavaObject(Projection.class) ;

                Class cls = null;

                ProjectionProcessor projectionprocessor = null;
                try {
                    cls = Class.forName(projection.getType());
                    projectionprocessor = (ProjectionProcessor) cls.newInstance();
                    JSONObject to = new JSONObject((Map<String, Object>) topologyList.get(name));
                    Topology topology = to.toJavaObject(Topology.class);
                    if(topology.getParallelism()!=0 ){

                        projection.setParallelism(topology.getParallelism());
                    }
                    else{
                        projection.setParallelism((Integer) yamlEntity.getApplication().getOrDefault("parallelism",1));
                    }
                    projection.setName(name);


                    event = projectionprocessor.projectionProcessorFunction(streamlineEventSingleOutputStreamOperator, projection);

                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
            else{


                //    List<com.zdjizhi.base.common.aggregate_function> functionlist = ppr.getJSONArray("functionlist").toJavaList(com.zdjizhi.base.common.aggregate_function.class);
                Aggregate aggregate = jsonObject.toJavaObject(Aggregate.class) ;
                Class cls = null;

                AggregateProcessor aggregateProcessor = null;
                try {
                    cls = Class.forName(aggregate.getType());

                    aggregateProcessor = (AggregateProcessor) cls.newInstance();
                    event = aggregateProcessor.aggregateProcessorFunction(streamlineEventSingleOutputStreamOperator, aggregate);

                    if(aggregate.getParallelism()!=0 ){

                        aggregate.setParallelism(aggregate.getParallelism());
                    }
                    else{
                        aggregate.setParallelism((Integer) yamlEntity.getApplication().getOrDefault("parallelism",1));
                    }
                    aggregate.setName(name);

                } catch (Exception e) {
                    e.printStackTrace();
                }




            }




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
}