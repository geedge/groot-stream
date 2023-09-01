package com.geedgenetworks.udf.io.async.state;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.io.IOException;

/**
 * An implementation of the Cached interface that uses an in-memory heap-based cache.
 * This class provides methods for getting, setting, and closing the cache.
 *
 * @param <K> The type of keys in the cache.
 * @param <V> The type of values in the cache.
 * @author chaoc
 * @since 1.0
 */
public class HeapCached<K, V> implements Cached<K, V> {

    private final Cache<K, V> store;

    /**
     * Constructs a new HeapCached instance with default cache settings.
     */
    public HeapCached() {
        this.store = CacheBuilder.newBuilder()
                .build();
    }

    /**
     * Retrieves the value associated with the specified key from the cache.
     *
     * @param k The key to retrieve the value for.
     * @return The value associated with the key, or null if the key is not present in the cache.
     */
    @Override
    public V get(K k) {
        return store.getIfPresent(k);
    }

    /**
     * Associates the specified value with the specified key in the cache.
     *
     * @param k The key to set the value for.
     * @param v The value to be associated with the key.
     */
    @Override
    public void set(K k, V v) {
        store.put(k, v);
    }

    @Override
    public void remove(K k) {
        store.invalidate(k);
    }

    /**
     * Cleans up and closes the cache.
     *
     * @throws IOException If an I/O error occurs during cache cleanup.
     */
    @Override
    public void close() throws IOException {
        this.store.cleanUp();
    }
}
