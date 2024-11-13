package com.geedgenetworks.core.connector.inline;

import com.geedgenetworks.api.connector.source.SourceProvider;
import com.geedgenetworks.api.connector.serialization.DecodingFormat;
import com.geedgenetworks.api.connector.event.Event;
import com.geedgenetworks.api.connector.type.StructType;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.List;

public class InlineSourceProvider implements SourceProvider {
    private final StructType physicalDataType;
    private final DeserializationSchema<Event> deserialization;
    private final List<byte[]> datas;
    private final long intervalPerRow;
    private final int repeatCount;

    public InlineSourceProvider(StructType physicalDataType, DecodingFormat decodingFormat, List<byte[]> datas, long intervalPerRow, int repeatCount) {
        this.physicalDataType = physicalDataType;
        this.deserialization = decodingFormat.createRuntimeDecoder(physicalDataType);
        this.datas = datas;
        this.intervalPerRow = intervalPerRow;
        this.repeatCount = repeatCount;
    }

    @Override
    public SingleOutputStreamOperator<Event> produceDataStream(StreamExecutionEnvironment env) {
        return env.addSource(new RichParallelSourceFunction<Event>() {
            volatile boolean stop;

            @Override
            public void open(Configuration parameters) throws Exception {
                deserialization.open(null);
            }

            @Override
            public void run(SourceContext<Event> ctx) throws Exception {
                int repeat = 0;
                while (!stop && (repeatCount < 0 || repeat < repeatCount)){
                    for (byte[] data : datas) {
                        if(stop){
                            return;
                        }
                        Event event = deserialization.deserialize(data);
                        event.getExtractedFields().put(Event.INTERNAL_TIMESTAMP_KEY, System.currentTimeMillis());
                        ctx.collect(event);

                        if(intervalPerRow > 0){
                            Thread.sleep(intervalPerRow);
                        }
                    }
                    repeat++;
                }
            }

            @Override
            public void cancel() {
                stop = true;
            }
        });
    }

    @Override
    public StructType getPhysicalDataType() {
        return physicalDataType;
    }
}
