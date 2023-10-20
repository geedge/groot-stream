package com.geedgenetworks.utils.hbase.async;

import org.apache.flink.api.common.time.Time;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * An abstract class representing a fixed-rate data fetcher that implements the ScheduledSupport,
 * Fetcher, and Closeable interfaces. This class provides the infrastructure for fetching data at a
 * fixed rate.
 *
 * @author chaoc
 * @since 1.0
 */
public abstract class FixedRateFetcher implements ScheduledSupport, Fetcher, Closeable {

    private static final Time DEFAULT_FETCH_INTERVAL = Time.minutes(5);
    private final ScheduledFuture<?> scheduledFuture;

    /**
     * Constructs a FixedRateFetcher with the specified fetch interval.
     *
     * @param fetchInterval The time interval between data fetches.
     */
    public FixedRateFetcher(final Time fetchInterval) {
        long intervalMs = fetchInterval.toMilliseconds();
        final ScheduledExecutorService service = getScheduledExecutorService();
        scheduledFuture =
                service.scheduleAtFixedRate(
                        this.asyncFetch(), 0, intervalMs, TimeUnit.MICROSECONDS);
    }

    /** Constructs a FixedRateFetcher with the default fetch interval. */
    public FixedRateFetcher() {
        this(DEFAULT_FETCH_INTERVAL);
    }

    /**
     * Closes the FixedRateFetcher and cancels the scheduled data fetch task.
     *
     * @throws IOException If an I/O error occurs during the close operation.
     */
    @Override
    public void close() throws IOException {
        scheduledFuture.cancel(true);
    }
}
