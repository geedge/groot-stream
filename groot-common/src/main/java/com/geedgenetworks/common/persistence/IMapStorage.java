package com.geedgenetworks.common.persistence;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public interface IMapStorage {
    public void initialize(Map<String, Object> properties);

    /**
     * Store a key-value pair in the map. todo: it's better add timeout parameter
     *
     * @param key storage key
     * @param value storage value
     * @return storage status, true is success, false is fail
     */
    public boolean store(Object key, Object value);

    /**
     * Store a key-value pair in the map storage.
     *
     * @param map storage key-value pair
     * @return if some key-value pair is not stored, return this keys; if all key-value pair is
     *     stored, return empty set.
     */
    public Set<Object> storeAll(Map<Object, Object> map);

    /**
     * Delete a key in the map storage.
     *
     * @param key storage key
     * @return storage status, true is success, false is fail
     */
    public boolean delete(Object key);

    /**
     * Delete a collection of keys from the map storage.
     *
     * @param keys delete keys
     * @return if some keys delete fail, will return this keys if all keys delete success, will
     *     return empty set
     */
    public Set<Object> deleteAll(Collection<Object> keys);

    public Map<Object, Object> loadAll() throws IOException;

    public Set<Object> loadAllKeys();

    public void destroy(boolean deleteAllFileFlag);
}
