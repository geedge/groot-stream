package com.geedgenetworks.udf.lookup;

import com.geedgenetworks.udf.io.HBaseClient;
import com.geedgenetworks.udf.io.HbaseConnectBuilder;
import com.geedgenetworks.udf.io.async.AsyncQueryExecutor;
import com.geedgenetworks.udf.io.async.Fetcher;
import com.geedgenetworks.udf.io.async.FixedRateFetcher;
import com.geedgenetworks.udf.io.async.state.Cached;
import com.geedgenetworks.udf.io.async.state.HeapCached;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;

/**
 * An abstract class representing an asynchronous query executor for HBase lookups with cache support.
 *
 * @param <K> The key type for the HBase lookup.
 * @param <V> The value type for the HBase lookup.
 * @author chaoc
 * @since 1.0
 */
public abstract class HbaseLookup<K, V> extends AsyncQueryExecutor<K, V> {

    private final Cached<K, V> cached;

    private final HBaseClient client;

    private final Fetcher fetcher;

    /**
     * Constructs a new HbaseLookup instance with the given configuration parameters.
     *
     * @param parameters The configuration parameters for HBase connection.
     */
    public HbaseLookup(final Configuration parameters) {
        // Establish an HBase connection using the provided parameter
        final Connection connection = HbaseConnectBuilder
                .newBuilder()
                .loadDefaultConfig()
                .build();
        this.client = new HBaseClient(connection);
        this.cached = new HeapCached<>();

        // Define the update function for cache refresh
        final Runnable func = () -> {
            Function<HBaseClient, Map<K, V>> update = getUpdate();
            Map<K, V> values = update.apply(client);
            values.forEach(cached::set);
        };

        // Create a fetcher for scheduled cache updates
        this.fetcher = new FixedRateFetcher() {
            @Override
            public Runnable asyncFetch() {
                return func;
            }
        };
    }

    /**
     * Closes the RadiusLookup instance and releases associated resources.
     *
     * @throws IOException If an I/O error occurs during the closing process.
     */
    @Override
    public void close() throws IOException {
        this.client.close();
        this.cached.close();
    }

    /**
     * Gets the cache associated with this RadiusLookup instance.
     *
     * @return The cache.
     */
    @Override
    protected Cached<K, V> getCache() {
        return this.cached;
    }

    /**
     * Gets the fetcher associated with this RadiusLookup instance.
     *
     * @return The fetcher.
     */
    @Override
    protected Fetcher getFetcher() {
        return this.fetcher;
    }

    /**
     * This method should be implemented by subclasses to define how data should be fetched and updated from HBase.
     *
     * @return A function that fetches and returns data from HBase as a Map.
     */
    protected abstract Function<HBaseClient, Map<K, V>> getUpdate();
}
