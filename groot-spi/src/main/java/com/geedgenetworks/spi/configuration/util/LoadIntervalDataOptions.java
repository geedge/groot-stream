package com.geedgenetworks.spi.configuration.util;

import java.io.Serializable;

public class LoadIntervalDataOptions implements Serializable {
    final String name;

    final long intervalMs;
    final boolean failOnException;
    final boolean updateDataOnStart;

    /**
     * @param name 名称, 用于日志打印以及线程名称标识
     * @param intervalMs 每隔多长时间更新数据
     * @param failOnException 更新数据时发生异常是否失败(默认false), 为true时如果发现异常data()方法下次返回数据时会抛出异常
     * @param updateDataOnStart start时是否先更新数据(默认true), 为false时start候intervalMs时间后才会第一个更新数据
     */
    private LoadIntervalDataOptions(String name, long intervalMs, boolean failOnException, boolean updateDataOnStart) {
        this.name = name;
        this.intervalMs = intervalMs;
        this.failOnException = failOnException;
        this.updateDataOnStart = updateDataOnStart;
    }

    public String getName() {
        return name;
    }

    public long getIntervalMs() {
        return intervalMs;
    }

    public boolean isFailOnException() {
        return failOnException;
    }

    public boolean isUpdateDataOnStart() {
        return updateDataOnStart;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static LoadIntervalDataOptions defaults(String name, long intervalMs) {
        return builder().withName(name).withIntervalMs(intervalMs).build();
    }

    public static final class Builder {
        private String name = "";
        private long intervalMs = 1000 * 60 * 10;
        private boolean failOnException = false;
        private boolean updateDataOnStart = true;

        public Builder withName(String name) {
            this.name = name;
            return this;
        }

        public Builder withIntervalMs(long intervalMs) {
            this.intervalMs = intervalMs;
            return this;
        }

        public Builder withFailOnException(boolean failOnException) {
            this.failOnException = failOnException;
            return this;
        }

        public Builder withUpdateDataOnStart(boolean updateDataOnStart) {
            this.updateDataOnStart = updateDataOnStart;
            return this;
        }

        public LoadIntervalDataOptions build() {
            return new LoadIntervalDataOptions(name, intervalMs, failOnException, updateDataOnStart);
        }
    }

}
