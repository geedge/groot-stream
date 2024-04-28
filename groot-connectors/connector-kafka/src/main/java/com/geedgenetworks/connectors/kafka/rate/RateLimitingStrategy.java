package com.geedgenetworks.connectors.kafka.rate;

import java.io.Serializable;

public interface RateLimitingStrategy extends Serializable {
    default boolean rateLimited(){
        return true;
    }

    long maxRate();

    RateLimitingStrategy withMaxRate(long maxRate);

    RateLimitingStatus record(long value);
}
