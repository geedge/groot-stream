package com.geedgenetworks.connectors.kafka;

import com.geedgenetworks.connectors.kafka.rate.RateLimitingStrategyType;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;
import java.util.List;

import static com.geedgenetworks.connectors.kafka.KafkaConnectorOptionsUtil.PROPERTIES_PREFIX;

public class KafkaConnectorOptions {
    public static final ConfigOption<List<String>> TOPIC =
            ConfigOptions.key("topic")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription("Topic names from which the table is read.");

    public static final ConfigOption<Boolean> LOG_FAILURES_ONLY =
            ConfigOptions.key("log.failures.only")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Optional flag to whether the producer should fail on errors, or only log them;\n"
                                    + "If this is set to true, then exceptions will be only logged, if set to false, exceptions will be eventually thrown, true by default.");

    public static final ConfigOption<RateLimitingStrategyType> RATE_LIMITING_STRATEGY =
            ConfigOptions.key("rate.limiting.strategy")
                    .enumType(RateLimitingStrategyType.class)
                    .defaultValue(RateLimitingStrategyType.NONE)
                    .withDescription("Rate limiting strategy, optionals:none,sliding_window, default none.");

    public static final ConfigOption<String> RATE_LIMITING_LIMIT_RATE =
            ConfigOptions.key( "rate.limiting.limit.rate")
                    .stringType()
                    .defaultValue("10Mbps")
                    .withDescription("Limit rate, examples: 10Mbps, 10Kbps, 10240bps.");

    public static final ConfigOption<Integer> RATE_LIMITING_WINDOW_SIZE =
            ConfigOptions.key("rate.limiting.window.size")
                    .intType()
                    .defaultValue(5)
                    .withDescription("window size, unit second, default 5 second.");

    public static final ConfigOption<Duration> RATE_LIMITING_BLOCK_DURATION =
            ConfigOptions.key("rate.limiting.block.duration")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(5))
                    .withDescription("对首次超出限流数据阻塞，最长阻塞多长时间后超出限流数据全部丢弃");

    public static final ConfigOption<Duration> RATE_LIMITING_BLOCK_RESET_DURATION =
            ConfigOptions.key("rate.limiting.block.reset.duration")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription("超速阻塞后速率恢复正常多长时间后重置超速阻塞状态");

    public static final ConfigOption<String> PROPS_BOOTSTRAP_SERVERS =
            ConfigOptions.key( PROPERTIES_PREFIX + "bootstrap.servers")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Required Kafka server connection string");



}
