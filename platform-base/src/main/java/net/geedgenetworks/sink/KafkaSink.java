package net.geedgenetworks.sink;

import com.alibaba.fastjson.JSON;
import net.geedgenetworks.pojo.GrootEvent;
import net.geedgenetworks.pojo.SinkConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import static net.geedgenetworks.utils.KafkaUtils.getKafkaSink;

public class KafkaSink implements Sink{




    @Override
    public void sink(SingleOutputStreamOperator<GrootEvent> streamlineEventSingleOutputStreamOperator, SinkConfig sinkConfig) throws Exception {
        streamlineEventSingleOutputStreamOperator.map(new MapFunction<GrootEvent, String>() {
            @Override
            public String map(GrootEvent value) throws Exception {

                String jsonString = JSON.toJSONString(value.getAuxiliaryFieldsAndValues());
                return jsonString;
            }
        }).setParallelism(sinkConfig.getParallelism()).addSink( getKafkaSink(sinkConfig.getProperties())).setParallelism(sinkConfig.getParallelism());
    }
}
