package com.geedgenetworks.common.utils.hbase.async;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Utility for scheduling tasks.
 *
 * @author chaoc
 * @since 1.0
 */
public interface ScheduledSupport {

    /**
     * Gets the default scheduled executor service.
     *
     * @return The singleton scheduled executor service.
     */
    default ScheduledExecutorService getScheduledExecutorService() {
        return DefaultScheduledExecutor.DEFAULT.getService();
    }

    /** Singleton implementation of scheduled executor service. */
    enum DefaultScheduledExecutor {
        DEFAULT(new ScheduledThreadPoolExecutor(4));

        private final ScheduledExecutorService service;

        DefaultScheduledExecutor(final ScheduledExecutorService service) {
            this.service = service;
        }

        /**
         * Gets the executor service.
         *
         * @return The scheduled executor service.
         */
        public ScheduledExecutorService getService() {
            return service;
        }
    }
}
