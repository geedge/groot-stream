package com.geedgenetworks.connectors.kafka.rate;

import java.io.Serializable;

// 专门用于实时统计发送字节速率, 输出最近一秒内速率
public class SlidingWindowRateStat implements Serializable {
    private static final long windowSize = 1000;
    private static final int bucketSize = 8;
    private static final int bucketMask = bucketSize - 1;
    private static final long bucketWindowSize = windowSize / bucketSize;
    private final long[] buckets;
    private int current;
    private long currentWindowEndMs;
    private long totalValue;

    public SlidingWindowRateStat() {
        this.buckets = new long[bucketSize];
        this.current = (bucketSize - 1) & bucketMask;
        this.currentWindowEndMs = System.currentTimeMillis() / bucketWindowSize * bucketWindowSize;
    }

    public void record(long value) {
        long timeMs = System.currentTimeMillis();
        currentUpdate(timeMs);
        buckets[current] += value;
        totalValue += value;
    }

    public long recordAndGetCurrentRate(long value) {
        long timeMs = System.currentTimeMillis();
        currentUpdate(timeMs);
        buckets[current] += value;
        totalValue += value;
        return totalValue;
    }

    public long getCurrentRate() {
        long timeMs = System.currentTimeMillis();
        currentUpdate(timeMs);
        return totalValue;
    }

    public long getCurrentRate(long timeMs) {
        currentUpdate(timeMs);
        return totalValue;
    }

    private void currentUpdate(long timeMs){
        if (timeMs > currentWindowEndMs) {
            do {
                current = (current + 1) & bucketMask;
                totalValue -= buckets[current];
                buckets[current] = 0;
                currentWindowEndMs += bucketWindowSize;
            } while (currentWindowEndMs < timeMs);
        }
    }
}
