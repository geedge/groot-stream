package com.geedgenetworks.core.utils.hbase;

import com.geedgenetworks.common.config.CommonConfig;
import com.geedgenetworks.common.config.CommonConfigOptions;
import com.github.rholder.retry.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * A builder class for creating HBase connections. This class provides methods for configuring and
 * building an HBase connection.
 *
 * @author chaoc
 * @since 1.0
 */
public class HbaseConnectBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(HbaseConnectBuilder.class);

    private final Configuration config;

    /** Creates a new HbaseConnectBuilder instance. */
    public static HbaseConnectBuilder newBuilder() {
        return new HbaseConnectBuilder();
    }

    // Private constructor for HbaseConnectBuilder.
    // Initializes the HBase Configuration.
    private HbaseConnectBuilder() {
        this.config = HBaseConfiguration.create();
    }

    /**
     * Loads default configuration settings.
     *
     * @return This HbaseConnectBuilder instance.
     */
    public HbaseConnectBuilder loadDefaultConfig() {
        this.config.set("hbase.zookeeper.quorum", CommonConfigOptions.ZOOKEEPER_QUORUM.defaultValue());
        return this;
    }

    /**
     * Initializes the HbaseConnectBuilder from a given Apache Flink configuration.
     *
     * @param configuration The Apache Flink configuration.
     * @return This HbaseConnectBuilder instance.
     */
    public HbaseConnectBuilder fromConfiguration(
            final org.apache.flink.configuration.Configuration configuration) {
        // TODO: Init Builder from Apache Flink configuration
        return this;
    }

    /**
     * Sets a configuration parameter for the HBase connection.
     *
     * @param name The name of the configuration parameter.
     * @param value The value of the configuration parameter.
     * @return This HbaseConnectBuilder instance.
     */
    public HbaseConnectBuilder set(String name, String value) {
        this.config.set(name, value);
        return this;
    }

    /**
     * Builds an HBase connection with the configured settings.
     *
     * @return The constructed HBase Connection.
     */
    public Connection build() {
        final Retryer<Connection> retryer =
                RetryerBuilder.<Connection>newBuilder()
                        .retryIfExceptionOfType(IOException.class)
                        .withStopStrategy(StopStrategies.stopAfterAttempt(3))
                        .withWaitStrategy(WaitStrategies.fixedWait(5, TimeUnit.SECONDS))
                        .build();
        try {
            LOG.info("Initializing the HBase connection");
            return retryer.call(() -> ConnectionFactory.createConnection(config));
        } catch (RetryException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
