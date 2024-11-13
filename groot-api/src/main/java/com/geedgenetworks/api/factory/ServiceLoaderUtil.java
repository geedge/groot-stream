package com.geedgenetworks.api.factory;

import java.util.*;

/** This class contains utilities to deal with {@link ServiceLoader}. */
class ServiceLoaderUtil {

    /**
     * This method behaves similarly to {@link ServiceLoader#load(Class, ClassLoader)}, but it
     * returns a list with the results of the iteration, wrapping the iteration failures such as
     * {@link NoClassDefFoundError}.
     */
    static <T> List<LoadResult<T>> load(Class<T> clazz, ClassLoader classLoader) {
        List<LoadResult<T>> loadResults = new ArrayList<>();

        Iterator<T> serviceLoaderIterator = ServiceLoader.load(clazz, classLoader).iterator();

        while (true) {
            try {
                T next = serviceLoaderIterator.next();
                loadResults.add(new LoadResult<>(next));
            } catch (NoSuchElementException e) {
                break;
            } catch (Throwable t) {
                loadResults.add(new LoadResult<>(t));
            }
        }

        return loadResults;
    }

    static class LoadResult<T> {
        private final T service;
        private final Throwable error;

        private LoadResult(T service, Throwable error) {
            this.service = service;
            this.error = error;
        }

        private LoadResult(T service) {
            this(service, null);
        }

        private LoadResult(Throwable error) {
            this(null, error);
        }

        public boolean hasFailed() {
            return error != null;
        }

        public Throwable getError() {
            return error;
        }

        public T getService() {
            return service;
        }
    }
}
