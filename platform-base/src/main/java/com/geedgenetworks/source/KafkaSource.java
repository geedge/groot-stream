package com.geedgenetworks.source;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.alibaba.fastjson2.JSON;
import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.pojo.SourceConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Map;

import static com.geedgenetworks.utils.ColumnUtil.columnSelector;
import static com.geedgenetworks.utils.KafkaUtils.timestampDeserializationConsumer;

public class KafkaSource implements Source {


    private static final Log logger = LogFactory.get();

    @Override
    public SingleOutputStreamOperator<Event> source(StreamExecutionEnvironment env, SourceConfig sourceConfig) throws Exception {
        DataStreamSource<Tuple2<String, Long>> sourceForSession = env.addSource(timestampDeserializationConsumer(sourceConfig.getProperties())).setParallelism(sourceConfig.getParallelism());
          SingleOutputStreamOperator<Event> streamEvent;
        streamEvent = sourceForSession.map(new MapFunction<Tuple2<String, Long>, Event>() {
            @Override
            public Event map(Tuple2<String, Long> message) {

                Event event = new Event();
                event.setFinal(true);
                try {
                    Map<String, Object> values = JSON.parseObject(message.f0);
                    values.put("common_recv_time",message.f1);
                    if(sourceConfig.getOutput_fields()!=null && sourceConfig.getOutput_fields().size()>0) {
                        values = columnSelector(values, sourceConfig.getOutput_fields());
                    }
                    event.setExtractedFields(values);
                    return event;
                } catch (Exception e) {
                    event.setFinal(false);
                    logger.error(e.toString());
                    return event;
                }
            }
        }).setParallelism(sourceConfig.getParallelism());

        if(sourceConfig.getWatermark_lag()!=null && sourceConfig.getWatermark_lag()>0L) {
            WatermarkStrategy<Event> strategyForSession = WatermarkStrategy
                    .<Event>forBoundedOutOfOrderness(Duration.ofMillis(sourceConfig.getWatermark_lag()))
                    .withTimestampAssigner((StreamlineEvent, timestamp) -> Integer.parseInt(String.valueOf(StreamlineEvent.getExtractedFields().get(sourceConfig.getWatermark_timestamp()))) * 1000);
            streamEvent.assignTimestampsAndWatermarks(strategyForSession);
        }

        return streamEvent;
    }
}
