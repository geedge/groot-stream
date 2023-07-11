package com.zdjizhi.base.sources;

import com.alibaba.fastjson.JSON;
import com.zdjizhi.base.pojo.GrootEvent;
import com.zdjizhi.base.pojo.SourceConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Map;

import static com.zdjizhi.base.utils.KafkaUtils.getKafkaConsumer;

public class KafkaSource implements Source {



    @Override
    public SingleOutputStreamOperator<GrootEvent> source(StreamExecutionEnvironment env, SourceConfig sourceConfig) throws Exception {
        DataStream<String> sourceForSession = env.addSource(getKafkaConsumer(sourceConfig.getKafka())).setParallelism(sourceConfig.getParallelism());
        WatermarkStrategy<GrootEvent> strategyForSession = WatermarkStrategy
                .<GrootEvent>forBoundedOutOfOrderness(Duration.ofSeconds(sourceConfig.getWatermark_time_lag()))
                .withTimestampAssigner((StreamlineEvent, timestamp) -> Integer.parseInt(String.valueOf(StreamlineEvent.getAuxiliaryFieldsAndValues().get(sourceConfig.getWatermark_timestamp()))) * 1000);

        SingleOutputStreamOperator<GrootEvent> streamEvent = sourceForSession.map(new MapFunction<String, GrootEvent>() {
            @Override
            public GrootEvent map(String message) {

                GrootEvent grootevent = new GrootEvent();
                grootevent.setFlag(true);
                try {

                    Map mapTypes = JSON.parseObject(message);
                    grootevent.setAuxiliaryFieldsAndValues(mapTypes);
                    return grootevent;
                } catch (Exception e) {
                    grootevent.setFlag(false);
                    return grootevent;

                }
            }
        }).setParallelism(sourceConfig.getParallelism()).assignTimestampsAndWatermarks(strategyForSession);


        return streamEvent;
    }
}
