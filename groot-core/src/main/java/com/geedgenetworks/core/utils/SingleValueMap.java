package com.geedgenetworks.core.utils;

import org.apache.flink.util.function.SupplierWithException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * 主要用于实现全局对象
 * 主要用于flink算子中，方便复用全局对象
 * flink算子中使用方法:
 *    // open方法中根据传入的获取data函数获取data
 *    data = SingleValueMap.acquireData("key", () -> getDataFunc());
 *    // process方法中使用data
 *    data.getData()
 *    // close方法中释放data
 *    if(data != null)
 *        data.release();
 */
public class SingleValueMap {
    static final Logger LOG = LoggerFactory.getLogger(SingleValueMap.class);
    private static Map<Object, Data<?>> cache = new LinkedHashMap<>();

    public static synchronized <T> Data<T> acquireData(Object key, SupplierWithException<T, Exception> dataSupplier) throws Exception {
        return acquireData(key, dataSupplier, x -> {});
    }

    public static synchronized <T> Data<T> acquireData(Object key, SupplierWithException<T, Exception> dataSupplier, Consumer<T> releaseFunc) throws Exception {
        assert releaseFunc != null;
        Data<?> existingData = cache.get(key);
        Data<T> data;
        if (existingData == null) {
            Data<T> newData = new Data<>(key, dataSupplier.get(), releaseFunc);
            cache.put(key, newData);
            data = newData;
        } else {
            data = (Data<T>) existingData;
        }
        data.useCnt += 1;

        LOG.info("acquireData: {}", data);

        return data;
    }

    private static synchronized <T> void releaseData(Data<T> data) {
        Data<?> cachedData = cache.get(data.key);
        if (cachedData == null) {
            LOG.error("can not get data: {}", data);
            return;
        }

        assert data == cachedData;
        LOG.info("releaseData: {}", data);

        data.useCnt -= 1;
        if (!data.inUse()) {
            data.destroy();
            cache.remove(data.key);

            LOG.info("removeData: {}", data);
        }
    }

    public static synchronized void clear() {
        Iterator<Map.Entry<Object, Data<?>>> iter = cache.entrySet().iterator();
        while (iter.hasNext()) {
            Data<?> data = iter.next().getValue();
            data.destroy();
            iter.remove();
        }
    }

    public final static class Data<T> {
        final Object key;
        final T data;
        final Consumer<T> destroyFunc;
        volatile int useCnt = 0;

        Data(Object key, T data, Consumer<T> destroyFunc) {
            this.key = key;
            this.data = data;
            this.destroyFunc = destroyFunc;
        }

        boolean inUse() {
            return useCnt > 0;
        }

        void destroy() {
            if (destroyFunc != null) {
                destroyFunc.accept(data);
            }
        }

        public void release() {
            releaseData(this);
        }

        public Object getKey() {
            return key;
        }

        public T getData() {
            return data;
        }

        @Override
        public String toString() {
            return "Data{" +
                    "key=" + key +
                    ", data=" + data +
                    ", useCnt=" + useCnt +
                    '}';
        }
    }
}
