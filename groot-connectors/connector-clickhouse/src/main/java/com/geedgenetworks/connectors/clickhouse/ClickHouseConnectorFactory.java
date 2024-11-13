package com.geedgenetworks.connectors.clickhouse;

import com.geedgenetworks.connectors.clickhouse.sink.EventBatchIntervalClickHouseSink;
import com.geedgenetworks.api.connector.sink.SinkProvider;
import com.geedgenetworks.api.factory.SinkFactory;
import com.geedgenetworks.api.event.Event;
import com.geedgenetworks.api.factory.FactoryUtil;
import com.geedgenetworks.api.factory.FactoryUtil.TableFactoryHelper;
import com.geedgenetworks.api.connector.schema.Schema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Preconditions;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static com.geedgenetworks.connectors.clickhouse.ClickHouseConnectorOptions.*;
import static com.geedgenetworks.connectors.clickhouse.ClickHouseConnectorOptionsUtil.*;

public class ClickHouseConnectorFactory implements SinkFactory {
    public static final String IDENTIFIER = "clickhouse";
    @Override
    public String type() {
        return IDENTIFIER;
    }

    @Override
    public SinkProvider getSinkProvider(Context context) {
        final TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        helper.validateExcept(CONNECTION_INFO_PREFIX); // 校验参数

        Schema schema = context.getSchema();
        ReadableConfig config = context.getConfiguration();

        String host = config.get(HOST);
        String table = config.get(TABLE);
        int batchSize = config.get(BATCH_SIZE);
        MemorySize batchByteMemorySize = config.get(BATCH_BYTE_SIZE);
        Preconditions.checkArgument(batchByteMemorySize.getMebiBytes() < 1000, "batch.byte.size can not bigger than 1000m");
        int batchByteSize = (int) batchByteMemorySize.getBytes();
        long batchIntervalMs = config.get(BATCH_INTERVAL).toMillis();
        Properties connInfo = getClickHouseConnInfo(context.getOptions());

        final SinkFunction<Event> sinkFunction = new EventBatchIntervalClickHouseSink(schema, batchSize, batchByteSize, batchIntervalMs, host, table, connInfo);

        return new SinkProvider() {
            @Override
            public DataStreamSink<?> consumeDataStream(DataStream<Event> dataStream) {
                return dataStream.addSink(sinkFunction);
            }

            @Override
            public boolean supportDynamicSchema() {
                return true;
            }
        };
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOST);
        options.add(TABLE);
        options.add(CONNECTION_USER);
        options.add(CONNECTION_PASSWORD);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(BATCH_SIZE);
        options.add(BATCH_BYTE_SIZE);
        options.add(BATCH_INTERVAL);
        return options;
    }

}
