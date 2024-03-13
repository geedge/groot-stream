package com.geedgenetworks.connectors.kafka;

import com.geedgenetworks.core.connector.format.DecodingFormat;
import com.geedgenetworks.core.connector.format.EncodingFormat;
import com.geedgenetworks.core.connector.sink.SinkProvider;
import com.geedgenetworks.core.connector.source.SourceProvider;
import com.geedgenetworks.core.factories.*;
import com.geedgenetworks.core.factories.FactoryUtil.TableFactoryHelper;
import com.geedgenetworks.core.types.StructType;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;

import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static com.geedgenetworks.connectors.kafka.KafkaConnectorOptions.*;
import static com.geedgenetworks.connectors.kafka.KafkaConnectorOptionsUtil.PROPERTIES_PREFIX;
import static com.geedgenetworks.connectors.kafka.KafkaConnectorOptionsUtil.getKafkaProperties;

public class KafkaTableFactory implements SourceTableFactory, SinkTableFactory {
    public static final String IDENTIFIER = "kafka";
    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public SourceProvider getSourceProvider(Context context) {
        final TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        // 获取valueDecodingFormat
        DecodingFormat valueDecodingFormat = helper.discoverDecodingFormat(DecodingFormatFactory.class, FactoryUtil.FORMAT);

        helper.validateExcept(PROPERTIES_PREFIX); // 校验参数，排除properties.参数

        StructType physicalDataType = context.getPhysicalDataType(); // 列类型
        ReadableConfig config = context.getConfiguration();

        List<String> topics = config.get(TOPIC);
        final Properties properties = getKafkaProperties(context.getOptions());

        return new KafkaSourceProvider(physicalDataType, valueDecodingFormat, topics, properties);
    }

    @Override
    public SinkProvider getSinkProvider(Context context) {
        final TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        // 获取valueEncodingFormat
        EncodingFormat valueEncodingFormat = helper.discoverEncodingFormat(EncodingFormatFactory.class, FactoryUtil.FORMAT);

        helper.validateExcept(PROPERTIES_PREFIX); // 校验参数，排除properties.参数

        StructType dataType = context.getDataType();
        ReadableConfig config = context.getConfiguration();

        String topic = config.get(TOPIC).get(0);
        boolean logFailuresOnly = config.get(LOG_FAILURES_ONLY);
        final Properties properties = getKafkaProperties(context.getOptions());

        return new KafkaSinkProvider(dataType, valueEncodingFormat, topic, properties, logFailuresOnly);
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(TOPIC);
        options.add(FactoryUtil.FORMAT);
        options.add(PROPS_BOOTSTRAP_SERVERS);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(LOG_FAILURES_ONLY);
        return options;
    }

}
