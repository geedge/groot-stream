package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.api.common.functions.RuntimeContext;

public interface RuntimeContextAware {
    void runtimeContextAware(RuntimeContext runtimeContext);
}
