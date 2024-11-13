package com.geedgenetworks.bootstrap.main.simple.collect;

import com.geedgenetworks.api.connector.sink.SinkProvider;
import com.geedgenetworks.api.factory.SinkFactory;
import com.geedgenetworks.api.event.Event;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;

import java.util.HashSet;
import java.util.Set;

/**
 * 用于测试的sink：把元素输出到标准输出或输出日志，输出类型：1(stdout),2(logInfo),3(logWarn)
 */
public class CollectTableFactory implements SinkFactory {
    public static final String IDENTIFIER = "collect";
    @Override
    public String type() {
        return IDENTIFIER;
    }

    @Override
    public SinkProvider getSinkProvider(Context context) {

        return new SinkProvider(){
            @Override
            public DataStreamSink<?> consumeDataStream(DataStream<Event> dataStream) {
                return dataStream.addSink(new CollectSink());
            }
        };
    }


    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        return options;
    }

}
