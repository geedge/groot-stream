package com.geedgenetworks.connectors.ipfix.collector;

import com.geedgenetworks.core.connector.source.SourceProvider;
import com.geedgenetworks.core.factories.FactoryUtil;
import com.geedgenetworks.core.factories.SourceTableFactory;
import com.geedgenetworks.core.types.StructType;
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
        options.add(IPFixConnectorOptions.INITIAL_PORT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(IPFixConnectorOptions.BUFFER_SIZE);
        options.add(IPFixConnectorOptions.PORT_NUMBER);
        options.add(IPFixConnectorOptions.RECEIVE_BUFFER);
        return options;
    }

}
