package com.geedgenetworks.connectors.kafka;

import com.geedgenetworks.api.connector.serialization.DecodingFormat;
import com.geedgenetworks.api.connector.serialization.EncodingFormat;
import com.geedgenetworks.connectors.kafka.rate.BlockDropRateLimitingStrategy;
import com.geedgenetworks.connectors.kafka.rate.NoRateLimitingStrategy;
import com.geedgenetworks.connectors.kafka.rate.RateLimitingStrategy;
import com.geedgenetworks.connectors.kafka.rate.RateLimitingStrategyType;
import com.geedgenetworks.api.connector.sink.SinkProvider;
import com.geedgenetworks.api.factory.SinkFactory;
import com.geedgenetworks.api.connector.source.SourceProvider;
import com.geedgenetworks.api.factory.SourceFactory;
import com.geedgenetworks.api.factory.DecodingFormatFactory;
import com.geedgenetworks.api.factory.EncodingFormatFactory;
import com.geedgenetworks.api.factory.FactoryUtil;
import com.geedgenetworks.api.connector.type.StructType;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.util.Preconditions;

import java.util.*;

import static com.geedgenetworks.connectors.kafka.KafkaConnectorOptions.*;
import static com.geedgenetworks.connectors.kafka.KafkaConnectorOptionsUtil.*;

public class KafkaConnectorFactory implements SourceFactory, SinkFactory {
    public static final String IDENTIFIER = "kafka";
    @Override
    public String type() {
        return IDENTIFIER;
    }

    @Override
    public SourceProvider getSourceProvider(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
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
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        // 获取valueEncodingFormat
        EncodingFormat valueEncodingFormat = helper.discoverEncodingFormat(EncodingFormatFactory.class, FactoryUtil.FORMAT);

        helper.validateExcept(PROPERTIES_PREFIX, HEADERS_PREFIX); // 校验参数，排除properties.参数

        StructType dataType = context.getDataType();
        ReadableConfig config = context.getConfiguration();

        String topic = config.get(TOPIC).get(0);
        boolean logFailuresOnly = config.get(LOG_FAILURES_ONLY);
        final Properties properties = getKafkaProperties(context.getOptions());
        Map<String, String> headers = getKafkaHeaders(context.getOptions());

        return new KafkaSinkProvider(dataType, valueEncodingFormat, topic, properties, headers, logFailuresOnly, getRateLimitingStrategy(config));
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
        options.add(RATE_LIMITING_STRATEGY);
        options.add(RATE_LIMITING_LIMIT_RATE);
        options.add(RATE_LIMITING_WINDOW_SIZE);
        options.add(RATE_LIMITING_BLOCK_DURATION);
        options.add(RATE_LIMITING_BLOCK_RESET_DURATION);
        return options;
    }

    private RateLimitingStrategy getRateLimitingStrategy(ReadableConfig config){
        RateLimitingStrategyType strategyType = config.get(RATE_LIMITING_STRATEGY);
        switch (strategyType){
            case NONE:
                return new NoRateLimitingStrategy();
            case SLIDING_WINDOW:
                return new BlockDropRateLimitingStrategy(
                        config.get(RATE_LIMITING_WINDOW_SIZE),
                        parseRateLimitingRate(config.get(RATE_LIMITING_LIMIT_RATE)),
                        config.get(RATE_LIMITING_BLOCK_DURATION).toMillis(),
                        config.get(RATE_LIMITING_BLOCK_RESET_DURATION).toMillis());
            default:
                throw new IllegalArgumentException("not supported strategy:" + strategyType);
        }
    }

    private long parseRateLimitingRate(String text){
        Preconditions.checkNotNull(text);
        final String trimmed = text.trim();
        Preconditions.checkArgument(!trimmed.isEmpty());

        final int len = trimmed.length();
        int pos = 0;

        char current;
        while (pos < len && (current = trimmed.charAt(pos)) >= '0' && current <= '9') {
            pos++;
        }

        final String number = trimmed.substring(0, pos);
        final String unit = trimmed.substring(pos).trim().toLowerCase();

        if (number.isEmpty()) {
            throw new NumberFormatException("text does not start with a number");
        }

        final long value;
        try {
            value = Long.parseLong(number); // this throws a NumberFormatException on overflow
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("The value '" + number+ "' cannot be re represented as long.");
        }

        long multiplier;
        if("mbps".equals(unit)){
            multiplier = 1 << 20;
        }else if("kbps".equals(unit)){
            multiplier = 1 << 10;
        }else if("bps".equals(unit)){
            multiplier = 1;
        }else if(unit.isEmpty()){
            multiplier = 1;
        }else{
            throw new IllegalArgumentException(text);
        }

        // bit单位转为byte单位
        return value * multiplier / 8;
    }
}
