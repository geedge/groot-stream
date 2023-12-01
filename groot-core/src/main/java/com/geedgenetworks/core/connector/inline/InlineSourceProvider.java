package com.geedgenetworks.core.connector.inline;

import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.connector.format.DecodingFormat;
import com.geedgenetworks.core.connector.source.SourceProvider;
import com.geedgenetworks.core.types.StructType;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class InlineSourceProvider implements SourceProvider {
    private final StructType physicalDataType;
    private final DeserializationSchema<Event> deserialization;
    private final byte[] data;

    public InlineSourceProvider(StructType physicalDataType, DecodingFormat decodingFormat, byte[] data) {
        this.physicalDataType = physicalDataType;
        this.deserialization = decodingFormat.createRuntimeDecoder(physicalDataType);
        this.data = data;
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
                while (!stop){
                    Event event = deserialization.deserialize(data);
                    event.getExtractedFields().put(Event.INTERNAL_TIMESTAMP_KEY, System.currentTimeMillis());
                    ctx.collect(event);

                    Thread.sleep(1000);
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
