package com.geedgenetworks.connectors.ipfix.collector;

import com.geedgenetworks.spi.source.SourceProvider;
import com.geedgenetworks.spi.source.SourceTableFactory;
import com.geedgenetworks.spi.table.factory.FactoryUtil;
import com.geedgenetworks.spi.table.type.StructType;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.geedgenetworks.connectors.ipfix.collector.IPFixConnectorOptionsUtil.PROPERTIES_PREFIX;

public class IPFixTableFactory implements SourceTableFactory {
    private static final String IDENTIFIER = "ipfix";

    @Override
    public SourceProvider getSourceProvider(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validateExcept(PROPERTIES_PREFIX);
        StructType physicalDataType = context.getPhysicalDataType();
        ReadableConfig config = context.getConfiguration();
        Map<String, String> options = context.getOptions();
        return new IPFixSourceProvider(physicalDataType, config, options);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(IPFixConnectorOptions.PORT_RANGE);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(IPFixConnectorOptions.MAX_PACKET_SIZE);
        options.add(IPFixConnectorOptions.MAX_RECEIVE_BUFFER_SIZE);
        return options;
    }

}
