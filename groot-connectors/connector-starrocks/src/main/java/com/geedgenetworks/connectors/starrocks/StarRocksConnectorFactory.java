package com.geedgenetworks.connectors.starrocks;


import com.geedgenetworks.api.connector.sink.SinkProvider;
import com.geedgenetworks.api.factory.SinkFactory;
import com.geedgenetworks.api.event.Event;
import com.geedgenetworks.api.factory.FactoryUtil;
import com.starrocks.connector.flink.table.sink.EventStarRocksDynamicSinkFunctionV2;
import com.starrocks.connector.flink.table.sink.SinkFunctionFactory;
import com.starrocks.connector.flink.table.sink.StarRocksSinkOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Preconditions;

import java.util.HashSet;
import java.util.Set;

public class StarRocksConnectorFactory implements SinkFactory {
    public static final String IDENTIFIER = "starrocks";
    @Override
    public String type() {
        return IDENTIFIER;
    }

    @Override
    public boolean supportsType(String type) {
        return IDENTIFIER.equalsIgnoreCase(type);
    }

    @Override
    public SinkProvider getSinkProvider(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validateExcept(CONNECTION_INFO_PREFIX);

        final boolean logFailuresOnly = context.getConfiguration().get(LOG_FAILURES_ONLY);
        StarRocksSinkOptions.Builder builder = StarRocksSinkOptions.builder();
        context.getOptions().forEach((key, value) -> {
            if(key.startsWith(CONNECTION_INFO_PREFIX)){
                builder.withProperty(key.substring(CONNECTION_INFO_PREFIX.length()), value);
            }
        });
        builder.withProperty("sink.properties.format", "json");
        final StarRocksSinkOptions options = builder.build();
        SinkFunctionFactory.detectStarRocksFeature(options);
        Preconditions.checkArgument(options.isSupportTransactionStreamLoad());
        final SinkFunction<Event> sinkFunction = new EventStarRocksDynamicSinkFunctionV2(options, logFailuresOnly);
        return new SinkProvider() {
            @Override
            public DataStreamSink<?> consumeDataStream(DataStream<Event> dataStream) {
                /*DataStream<String> ds = dataStream.flatMap(new FlatMapFunction<Event, String>() {
                    @Override
                    public void flatMap(Event value, Collector<String> out) throws Exception {
                        try {
                            out.collect(JSON.toJSONString(value.getExtractedFields()));
                        } catch (Exception e) {
                           e.printStackTrace();
                        }
                    }
                });
                SinkFunction<String> sink = StarRocksSink.sink(options);
                return ds.addSink(sink);
                */
                return dataStream.addSink(sinkFunction);
            }
        };
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(LOG_FAILURES_ONLY);
        return options;
    }

    public static final String CONNECTION_INFO_PREFIX = "connection.";

    public static final ConfigOption<Boolean> LOG_FAILURES_ONLY =
            ConfigOptions.key("log.failures.only")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Optional flag to whether the sink should fail on errors, or only log them;\n"
                            + "If this is set to true, then exceptions will be only logged, if set to false, exceptions will be eventually thrown, true by default.");
}
