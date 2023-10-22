package com.geedgenetworks.connectors.clickhouse.sink;

import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.SinkConfig;
import com.geedgenetworks.core.sink.Sink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.TimeUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ClickHouseSink implements Sink {

    @Override
    public void sink(SingleOutputStreamOperator<Event> operator, SinkConfig sinkConfig)
            throws Exception {
        ParameterTool parameter = getParameterTool(sinkConfig.getProperties());
        int batchSize = parameter.getInt("batch_size", 100000);
        long batchIntervalMs =
                TimeUtils.parseDuration(parameter.get("batch_interval", "30s")).toMillis();
        String host = parameter.get("host");
        String table = parameter.get("table");
        Properties connInfo = getConnInfo(sinkConfig.getProperties());
        EventBatchIntervalClickHouseSink sink =
                new EventBatchIntervalClickHouseSink(
                        batchSize, batchIntervalMs, host, table, connInfo);
        operator.addSink(sink)
                .setParallelism(
                        sinkConfig.getParallelism() > 0
                                ? sinkConfig.getParallelism()
                                : operator.getExecutionEnvironment().getParallelism())
                .name(sinkConfig.getName());
    }

    private ParameterTool getParameterTool(Map<String, Object> config) {
        Map<String, String> params = new HashMap<>();
        for (Map.Entry<String, Object> entry : config.entrySet()) {
            params.put(entry.getKey(), entry.getValue().toString());
            System.out.println(entry.getKey() + " -> " + entry.getValue().toString());
        }
        return ParameterTool.fromMap(params);
    }

    private Properties getConnInfo(Map<String, Object> config) {
        Properties connInfo = new Properties();
        int len = "connection.".length();
        for (Map.Entry<String, Object> entry : config.entrySet()) {
            if (entry.getKey().startsWith("connection.")) {
                connInfo.put(entry.getKey().substring(len), entry.getValue().toString());
            }
        }
        return connInfo;
    }
}
