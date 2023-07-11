package com.zdjizhi.schedule;

import com.alibaba.fastjson.JSONObject;
import com.zdjizhi.base.filters.FilterImpl;
import com.zdjizhi.base.pipelines.aggregate.AggregateProcessor;
import com.zdjizhi.base.pipelines.projection.ProjectionProcessor;
import com.zdjizhi.base.pojo.*;
import com.zdjizhi.base.sinks.KafkaSink;
import com.zdjizhi.base.sources.KafkaSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class Execute {
    private static final Logger logger = LoggerFactory.getLogger(com.zdjizhi.schedule.Execute.class);

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.setString("rest.port", "9091");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf).getExecutionEnvironment();

       // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // sourcetest schedule = (sourcetest) cls.newInstance();


        // 指定YAML文件的路径



        String yamlFilePath = Objects.requireNonNull(Thread.currentThread().getContextClassLoader().getResource("business.yaml")).getPath();



        Yaml yaml = new Yaml();


/*        URL url = new URL("http://192.168.40.223:8186/hos/test5/yaml");
        //打开链接
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        //设置请求方式为"GET"
        conn.setRequestMethod("GET");
        //超时响应时间为5秒
        conn.setConnectTimeout(5 * 1000);*/
        //通过输入流获取图片数据
        Map<String, Object> yamlData = null;
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
        }
        LinkedHashMap<String, Object> source = (LinkedHashMap<String, Object>) yamlData.get("Sources");

        LinkedHashMap<String, Object> filter = (LinkedHashMap<String, Object>) yamlData.get("Filter");

        LinkedHashMap<String, Object> pre_processor = (LinkedHashMap<String, Object>) yamlData.get("Pre_processing_Pipelines");
        LinkedHashMap<String, Object> projection_processor = (LinkedHashMap<String, Object>) yamlData.get("Processing_Pipelines");
        LinkedHashMap<String, Object> sink = (LinkedHashMap<String, Object>) yamlData.get("Sinks");

        Map<String, Object> excuter = (Map<String, Object>) yamlData.get("excuter");


        LinkedHashMap<String, ArrayList<String>> list = (LinkedHashMap<String, ArrayList<String>>) excuter.get("Sources");

        LinkedHashMap<String, ArrayList<String>> pip = (LinkedHashMap<String, ArrayList<String>>) excuter.get("Operators");











        list.forEach((key, value) -> {

            if (source.containsKey(key)) {
                SingleOutputStreamOperator<GrootEvent> streamlineEventSingleOutputStreamOperator = null;
                JSONObject jo = new JSONObject((Map<String, Object>) source.get(key));
                switch (jo.getString("type")) {
                    case "kafka":
                        SourceConfig kafkaSourceConfig = jo.toJavaObject(SourceConfig.class);
                        String className = kafkaSourceConfig.getMain_class();
                        Class cls = null;
                        KafkaSource kafkaSource = null;
                        try {
                            cls = Class.forName(className);
                            kafkaSource = (KafkaSource) cls.newInstance();
                            kafkaSourceConfig.setParallelism(1);
                            streamlineEventSingleOutputStreamOperator = kafkaSource.source(env, kafkaSourceConfig);
                        } catch (InstantiationException e) {
                            e.printStackTrace();
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        } catch (ClassNotFoundException e) {
                            e.printStackTrace();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        break;
                    case "hbase":
                        break;
                    default:
                        break;
                }
                for (String v : value) {
                    excute(v, streamlineEventSingleOutputStreamOperator, pip,filter, pre_processor, projection_processor, sink);

                }


            } else {


            }


            System.out.println(value);
        });


        env.execute("testjob");


    }

    private static void excute(String name, SingleOutputStreamOperator<GrootEvent> streamlineEventSingleOutputStreamOperator, LinkedHashMap<String, ArrayList<String>> pip,LinkedHashMap<String, Object> filter, LinkedHashMap<String, Object> pre_processor, LinkedHashMap<String, Object> projection_processor, LinkedHashMap<String, Object> sink) {


        SingleOutputStreamOperator<GrootEvent> vv = test(name, streamlineEventSingleOutputStreamOperator,filter, pre_processor, projection_processor, sink);

        for (String s : pip.get(name)) {

            excute(s, vv, pip,filter, pre_processor, projection_processor, sink);

        }
    }

    private static SingleOutputStreamOperator<GrootEvent> test(String name, SingleOutputStreamOperator<GrootEvent> streamlineEventSingleOutputStreamOperator,LinkedHashMap<String, Object> filter, LinkedHashMap<String, Object> pre_processor, LinkedHashMap<String, Object> projection_processor, LinkedHashMap<String, Object> sink) {


        SingleOutputStreamOperator<GrootEvent> event = null;

        if (filter.containsKey(name)) {
            JSONObject jsonObject = new JSONObject((Map<String, Object>) filter.get(name));
            Rule rule = jsonObject.toJavaObject(com.zdjizhi.base.pojo.Rule.class);
           // List<com.zdjizhi.base.common.regular_rule> listRule = f.getJSONArray("rule_list").toJavaList(com.zdjizhi.base.common.regular_rule.class);

            String className = jsonObject.getString("main_class");
            Class cls = null;
            FilterImpl filterImpl = null;
            try {
                cls = Class.forName(className);
                filterImpl = (FilterImpl) cls.newInstance();

                rule.setParallelism(1);
                event = filterImpl.filterFunction(streamlineEventSingleOutputStreamOperator, rule);

            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }


        } else if (pre_processor.containsKey(name)) {
            JSONObject jsonObject = new JSONObject((Map<String, Object>) pre_processor.get(name));


            if("projection".equals(jsonObject.getString("type"))){


                //   List<com.zdjizhi.base.common.projection_function> functionlist = ppr.getJSONArray("functionlist").toJavaList(com.zdjizhi.base.common.projection_function.class);


                Projection projection = jsonObject.toJavaObject(com.zdjizhi.base.pojo.Projection.class) ;
                projection.setParallelism(2);
                Class cls = null;

                ProjectionProcessor projectionprocessor = null;
                try {
                    cls = Class.forName(projection.getMain_class());
                    projectionprocessor = (ProjectionProcessor) cls.newInstance();
                    event = projectionprocessor.projectionProcessorFunction(streamlineEventSingleOutputStreamOperator, projection);

                } catch (InstantiationException e) {
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
            else{


                //    List<com.zdjizhi.base.common.aggregate_function> functionlist = ppr.getJSONArray("functionlist").toJavaList(com.zdjizhi.base.common.aggregate_function.class);
                Aggregate aggregate = jsonObject.toJavaObject(com.zdjizhi.base.pojo.Aggregate.class) ;
                aggregate.setParallelism(3);
                Class cls = null;

                AggregateProcessor aggregateProcessor = null;
                try {
                    cls = Class.forName(aggregate.getMain_class());

                    aggregateProcessor = (AggregateProcessor) cls.newInstance();
                    event = aggregateProcessor.aggregateProcessorFunction(streamlineEventSingleOutputStreamOperator, aggregate);

                } catch (InstantiationException e) {
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }




            }


        } else if (projection_processor.containsKey(name)) {

            JSONObject jsonObject = new JSONObject((Map<String, Object>) projection_processor.get(name));
       if("projection".equals(jsonObject.getString("type"))){


             //   List<com.zdjizhi.base.common.projection_function> functionlist = ppr.getJSONArray("functionlist").toJavaList(com.zdjizhi.base.common.projection_function.class);


           Projection projection = jsonObject.toJavaObject(com.zdjizhi.base.pojo.Projection.class) ;
           projection.setParallelism(4);
                Class cls = null;

                ProjectionProcessor projectionprocessor = null;
                try {
                    cls = Class.forName(projection.getMain_class());
                    projectionprocessor = (ProjectionProcessor) cls.newInstance();
                    event = projectionprocessor.projectionProcessorFunction(streamlineEventSingleOutputStreamOperator, projection);

                } catch (InstantiationException e) {
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }

       }
       else{


           //    List<com.zdjizhi.base.common.aggregate_function> functionlist = ppr.getJSONArray("functionlist").toJavaList(com.zdjizhi.base.common.aggregate_function.class);
           Aggregate aggregate = jsonObject.toJavaObject(com.zdjizhi.base.pojo.Aggregate.class) ;
           aggregate.setParallelism(1);

               Class cls = null;

           AggregateProcessor aggregateProcessor = null;
               try {
                   cls = Class.forName(aggregate.getMain_class());

                   aggregateProcessor = (AggregateProcessor) cls.newInstance();
                   event = aggregateProcessor.aggregateProcessorFunction(streamlineEventSingleOutputStreamOperator, aggregate);

               } catch (InstantiationException e) {
                   e.printStackTrace();
               } catch (IllegalAccessException e) {
                   e.printStackTrace();
               } catch (ClassNotFoundException e) {
                   e.printStackTrace();
               } catch (Exception e) {
                   e.printStackTrace();
               }




       }




        } else if (sink.containsKey(name)) {

            JSONObject jsonObject = new JSONObject((Map<String, Object>) sink.get(name));

            switch (jsonObject.getString("type")) {
                case "kafka":
                    SinkConfig kafkaSinkConfig = jsonObject.toJavaObject(SinkConfig.class);
                    kafkaSinkConfig.setParallelism(5);
                    String sinkclassName = kafkaSinkConfig.getMain_class();
                    Class cls = null;
                    KafkaSink kafkaSink = null;
                    try {
                        cls = Class.forName(sinkclassName);

                        kafkaSink = (KafkaSink) cls.newInstance();
                        kafkaSink.sink(streamlineEventSingleOutputStreamOperator, kafkaSinkConfig);

                    } catch (InstantiationException e) {
                        e.printStackTrace();
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    break;
                case "hbase":
                    break;
                default:
                    break;


            }
        }
        return event;
    }
}