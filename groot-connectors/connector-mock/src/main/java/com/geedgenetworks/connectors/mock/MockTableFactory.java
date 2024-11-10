package com.geedgenetworks.connectors.mock;

import com.geedgenetworks.connectors.mock.faker.FakerUtils;
import com.geedgenetworks.connectors.mock.faker.ObjectFaker;
import com.geedgenetworks.spi.source.SourceProvider;
import com.geedgenetworks.spi.source.SourceTableFactory;
import com.geedgenetworks.spi.table.event.Event;
import com.geedgenetworks.spi.table.factory.FactoryUtil;
import com.geedgenetworks.spi.table.type.StructType;
import org.apache.commons.io.FileUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

import static com.geedgenetworks.connectors.mock.MockConnectorOptions.*;

public class MockTableFactory implements SourceTableFactory {
    public static final String IDENTIFIER = "mock";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public SourceProvider getSourceProvider(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        final StructType physicalDataType = context.getPhysicalDataType();
        ReadableConfig config = context.getConfiguration();

        final String mockDescFilePath = config.get(MOCK_DESC_FILE_PATH).trim();
        final int rowsPerSecond = config.get(ROWS_PER_SECOND);
        final long numberOfRows = config.get(NUMBER_OF_ROWS);
        final long millisPerRow = config.get(MILLIS_PER_ROW);

        return new SourceProvider() {
            @Override
            public SingleOutputStreamOperator<Event> produceDataStream(StreamExecutionEnvironment env) {
                return env.addSource(new MockSource(parseFaker(mockDescFilePath), rowsPerSecond, numberOfRows, millisPerRow));
            }

            @Override
            public StructType getPhysicalDataType() {
                return physicalDataType;
            }
        };
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(MOCK_DESC_FILE_PATH);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(ROWS_PER_SECOND);
        options.add(NUMBER_OF_ROWS);
        options.add(MILLIS_PER_ROW);
        return options;
    }

    private ObjectFaker parseFaker(String mockDescFilePath){
        try {
            String json = FileUtils.readFileToString(new File(mockDescFilePath), StandardCharsets.UTF_8);
            return FakerUtils.parseObjectFakerFromJson(json);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
