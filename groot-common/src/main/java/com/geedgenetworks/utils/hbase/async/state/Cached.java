package com.geedgenetworks.utils.hbase.async.state;

import java.io.Closeable;


/**
 * An interface for a generic caching mechanism. Implementations of this interface
 * are expected to provide methods for retrieving and setting cached values.
 *
 * @param <K> The type of keys used to access cached values.
 * @param <V> The type of cached values.
 * @author chaoc
 * @since 1.0
 */
public interface Cached<K, V> extends Closeable {

    /**
     * Retrieves a cached value associated with the given key.
     *
     * @param k The key used to access the cached value.
     * @return The cached value associated with the key, or null if it is not present.
     */
    V get(K k);

    /**
     * Sets a cached value for the given key.
     *
     * @param k The key to associate with the cached value.
     * @param v The value to be cached.
     */
    void set(K k, V v);

    /**
     * Remove a cached value for the given key.
     *
     * @param k The key to associate with the cached value.
     */
    void remove(K k);
}
