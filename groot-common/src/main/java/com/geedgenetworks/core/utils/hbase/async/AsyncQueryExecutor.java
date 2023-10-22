package com.geedgenetworks.core.utils.hbase.async;

import com.geedgenetworks.core.utils.hbase.async.state.Cached;

/**
 * An abstract class for executing asynchronous queries and caching the results. This class
 * implements the Cached interface and provides methods for getting and setting values in the cache.
 *
 * @param <K> The type of keys used for caching.
 * @param <V> The type of values stored in the cache.
 * @author chaoc
 * @since 1.0
 */
public abstract class AsyncQueryExecutor<K, V> implements Cached<K, V> {

    /**
     * Retrieves a value from the cache.
     *
     * @param k The key for the cached value.
     * @return The cached value associated with the key.
     */
    @Override
    public final V get(K k) {
        return getCache().get(k);
    }

    /**
     * Sets a value in the cache.
     *
     * @param k The key for the cached value.
     * @param v The value to be cached.
     */
    @Override
    public final void set(K k, V v) {
        getCache().set(k, v);
    }

    /**
     * Remove a value from the cache.
     *
     * @param k The key for the cached value.
     */
    @Override
    public void remove(K k) {
        getCache().remove(k);
    }

    /**
     * Gets the cache used for storing query results.
     *
     * @return The cache instance used for caching query results.
     */
    protected abstract Cached<K, V> getCache();

    /**
     * Gets the fetcher used for executing asynchronous queries.
     *
     * @return The fetcher instance used for executing queries.
     */
    protected abstract Fetcher getFetcher();
}
