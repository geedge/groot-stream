package com.geedgenetworks.connectors.kafka.rate;

public class NoRateLimitingStrategy implements RateLimitingStrategy {
    @Override
    public boolean rateLimited() {
        return false; // 实际不限速
    }

    @Override
    public long maxRate() {
        return Long.MAX_VALUE;
    }

    @Override
    public NoRateLimitingStrategy withMaxRate(long maxRate) {
        return this;
    }

    @Override
    public RateLimitingStatus record(long value) {
        return RateLimitingStatus.NONE;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }
}
