package com.starrocks.connector.flink.table.sink;

import com.geedgenetworks.api.metrics.InternalMetrics;
import com.starrocks.connector.flink.manager.StarRocksStreamLoadListener;
import com.starrocks.data.load.stream.StreamLoadResponse;
import org.apache.flink.api.common.functions.RuntimeContext;

public class EventStreamLoadListener extends StarRocksStreamLoadListener {
    private transient InternalMetrics internalMetrics;
    public EventStreamLoadListener(RuntimeContext context, StarRocksSinkOptions sinkOptions, InternalMetrics internalMetrics) {
        super(context, sinkOptions);
        this.internalMetrics = internalMetrics;
    }

    @Override
    public void flushSucceedRecord(StreamLoadResponse response) {
        super.flushSucceedRecord(response);
        if (response.getFlushRows() != null) {
            internalMetrics.incrementOutEvents(response.getFlushRows());
        }
    }

    @Override
    public void flushFailedRecord() {
        super.flushFailedRecord();
        internalMetrics.incrementErrorEvents(1);
    }
}
