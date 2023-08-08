package com.geedgenetworks.source;

import com.alibaba.fastjson.JSON;
import com.geedgenetworks.pojo.SourceConfig;
import com.geedgenetworks.pojo.Event;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Map;

import static com.geedgenetworks.utils.KafkaUtils.getKafkaConsumer;

public class KafkaSource implements Source {



    @Override
    public SingleOutputStreamOperator<Event> source(StreamExecutionEnvironment env, SourceConfig sourceConfig) throws Exception {
        DataStream<String> sourceForSession = env.addSource(getKafkaConsumer(sourceConfig.getProperties())).setParallelism(sourceConfig.getParallelism());
        WatermarkStrategy<Event> strategyForSession = WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofMillis(sourceConfig.getWatermark_lag()))
                .withTimestampAssigner((StreamlineEvent, timestamp) -> Integer.parseInt(String.valueOf(StreamlineEvent.getExtractedFields().get(sourceConfig.getWatermark_timestamp()))) * 1000);

        SingleOutputStreamOperator<Event> streamEvent = sourceForSession.map(new MapFunction<String, Event>() {
            @Override
            public Event map(String message) {

                Event grootevent = new Event();
                grootevent.setFinal(true);
                try {

                    Map mapTypes = JSON.parseObject(message);
                    grootevent.setExtractedFields(mapTypes);
                    return grootevent;
                } catch (Exception e) {
                    grootevent.setFinal(false);
                    return grootevent;

                }
            }
        }).setParallelism(sourceConfig.getParallelism()).assignTimestampsAndWatermarks(strategyForSession);


        return streamEvent;
    }
}
