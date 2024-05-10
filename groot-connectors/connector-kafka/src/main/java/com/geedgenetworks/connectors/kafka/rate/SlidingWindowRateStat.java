package com.geedgenetworks.connectors.kafka.rate;

import org.apache.flink.util.Preconditions;

import java.io.Serializable;

// 专门用于实时统计发送字节速率, 输出最近一秒内速率
public class SlidingWindowRateStat implements Serializable {
    private final long windowSecond;
    private final long windowSize;
    private final int bucketSize;
    private final long bucketWindowSize;
    private final long[] buckets;
    private int current;
    private long currentWindowEndMs;
    private long totalValue;

    public SlidingWindowRateStat() {
        this(5);
    }

    public SlidingWindowRateStat(int windowSecond) {
        Preconditions.checkArgument(windowSecond > 0 && windowSecond <= 60);
        this.windowSecond = windowSecond;
        this.windowSize = windowSecond * 1000;
        this.bucketWindowSize = 100;
        this.bucketSize = (int) (windowSize / bucketWindowSize);
        this.buckets = new long[bucketSize];
        this.current = bucketSize - 1;
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
        return totalValue / windowSecond;
    }

    public long getCurrentRate() {
        long timeMs = System.currentTimeMillis();
        currentUpdate(timeMs);
        return totalValue / windowSecond;
    }

    public long getCurrentRate(long timeMs) {
        currentUpdate(timeMs);
        return totalValue / windowSecond;
    }

    private void currentUpdate(long timeMs){
        if (timeMs > currentWindowEndMs) {
            do {
                current = current + 1;
                if(current >= bucketSize){
                    current = 0;
                }
                totalValue -= buckets[current];
                buckets[current] = 0;
                currentWindowEndMs += bucketWindowSize;
            } while (currentWindowEndMs < timeMs);
        }
    }
}
