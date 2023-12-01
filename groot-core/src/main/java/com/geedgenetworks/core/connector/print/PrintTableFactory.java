package com.geedgenetworks.core.connector.print;

import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.connector.format.EncodingFormat;
import com.geedgenetworks.core.connector.sink.SinkProvider;
import com.geedgenetworks.core.factories.EncodingFormatFactory;
import com.geedgenetworks.core.factories.FactoryUtil;
import com.geedgenetworks.core.factories.SinkTableFactory;
import com.geedgenetworks.core.types.StructType;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;

import java.util.HashSet;
import java.util.Set;

/**
 * 用于测试的sink：把元素输出到标准输出或输出日志，输出类型：1(stdout),2(logInfo),3(logWarn)
 */
public class PrintTableFactory implements SinkTableFactory {
    public static final String IDENTIFIER = "print";
    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public SinkProvider getSinkProvider(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        // 获取encodingFormat
        EncodingFormat encodingFormat = helper.discoverEncodingFormat(EncodingFormatFactory.class, FactoryUtil.FORMAT);

        StructType dataType = context.getSchema();
        ReadableConfig config = context.getConfiguration();

        int type = config.get(TYPE);
        final PrintSinkFunction sinkFunction = new PrintSinkFunction(dataType, encodingFormat, type);
        return new SinkProvider(){

            @Override
            public DataStreamSink<?> consumeDataStream(DataStream<Event> dataStream) {
                return dataStream.addSink(sinkFunction);
            }
        };
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FactoryUtil.FORMAT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(TYPE);
        return options;
    }

    public static final ConfigOption<Integer> TYPE =
            ConfigOptions.key("type")
                    .intType()
                    .defaultValue(1)
                    .withDescription("输出类型:1(stdout),2(logInfo),3(logWarn)");
}
