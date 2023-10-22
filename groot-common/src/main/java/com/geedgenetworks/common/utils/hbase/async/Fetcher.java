package com.geedgenetworks.common.utils.hbase.async;

/**
 * An interface for defining a fetcher that performs asynchronous data retrieval. Implementing
 * classes should provide an implementation of the {@link #asyncFetch()} method.
 *
 * @author chaoc
 * @since 1.0
 */
public interface Fetcher {

    /**
     * Initiates an asynchronous data retrieval operation.
     *
     * @return A {@link Runnable} representing the asynchronous fetch task.
     */
    Runnable asyncFetch();
}
