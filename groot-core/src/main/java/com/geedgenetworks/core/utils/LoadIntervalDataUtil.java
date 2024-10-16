package com.geedgenetworks.core.utils;

import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flink.util.function.SupplierWithException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class LoadIntervalDataUtil<T> {
    static final Logger LOG = LoggerFactory.getLogger(LoadIntervalDataUtil.class);

    private final SupplierWithException<T, Exception> dataSupplier;
    private final LoadIntervalDataOptions options;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private ScheduledExecutorService scheduler;
    private Exception exception;
    private T data;

    private LoadIntervalDataUtil(SupplierWithException<T, Exception> dataSupplier, LoadIntervalDataOptions options) {
        this.dataSupplier = dataSupplier;
        this.options = options;
    }

    public static <T> LoadIntervalDataUtil<T> newInstance(SupplierWithException<T, Exception> dataSupplier, LoadIntervalDataOptions options) {
        LoadIntervalDataUtil<T> loadIntervalDataUtil = new LoadIntervalDataUtil(dataSupplier, options);
        loadIntervalDataUtil.start();
        return loadIntervalDataUtil;
    }

    public T data() throws Exception {
        if (!options.failOnException || exception == null) {
            return data;
        } else {
            throw exception;
        }
    }

    private void updateData() {
        try {
            LOG.info("{} updateData start....", options.name);
            data = dataSupplier.get();
            LOG.info("{} updateData end....", options.name);
        } catch (Throwable t) {
            if (options.failOnException) {
                exception = new RuntimeException(t);
            }
            LOG.info("{} updateData error", options.name, t);
        }
    }

    private void start() {
        if (started.compareAndSet(false, true)) {
            if (options.updateDataOnStart) {
                updateData();
            }
            this.scheduler = newDaemonSingleThreadScheduledExecutor(String.format("LoadIntervalDataUtil[%s]", options.name));
            this.scheduler.scheduleWithFixedDelay(() -> updateData(), options.intervalMs, options.intervalMs, TimeUnit.MILLISECONDS);
            LOG.info("{} start....", options.name);
        }
    }

    public void stop() {
        if (stopped.compareAndSet(false, true)) {
            if (scheduler != null) {
                this.scheduler.shutdown();
            }
            LOG.info("{} stop....", options.name);
        }
    }

    private static ScheduledExecutorService newDaemonSingleThreadScheduledExecutor(String threadName) {
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat(threadName).build();
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, threadFactory);
        // By default, a cancelled task is not automatically removed from the work queue until its delay
        // elapses. We have to enable it manually.
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }
}
