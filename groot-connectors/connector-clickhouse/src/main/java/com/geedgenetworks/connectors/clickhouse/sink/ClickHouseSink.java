package com.geedgenetworks.connectors.clickhouse.sink;

import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.pojo.SinkConfig;
import com.geedgenetworks.sink.Sink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.TimeUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ClickHouseSink implements Sink {

    @Override
    public void sink(SingleOutputStreamOperator<Event> operator, SinkConfig sinkConfig) throws Exception {
        ParameterTool parameter = getParameterTool(sinkConfig.getProperties());
        int batchSize = parameter.getInt("batch_size", 100000);
        long batchIntervalMs = TimeUtils.parseDuration(parameter.get("batch_interval", "30s")).toMillis();
        String[] urls = parameter.get("urls").split(",");
        String table = parameter.get("table");
        Properties connInfo = getConnInfo(sinkConfig.getProperties());
        /*connInfo.put("user", "default");
        connInfo.put("password", "galaxy2019");
        connInfo.put("database", "default");*/
        EventBatchIntervalClickHouseSink sink = new EventBatchIntervalClickHouseSink(batchSize, batchIntervalMs, urls, table, connInfo);
        operator.addSink(sink).setParallelism(sinkConfig.getParallelism()).name(sinkConfig.getName());
    }

    private ParameterTool getParameterTool(Map<String, Object> config){
        Map<String, String> params = new HashMap<>();
        for (Map.Entry<String, Object> entry : config.entrySet()) {
            params.put(entry.getKey(), entry.getValue().toString());
        }
        return ParameterTool.fromMap(params);
    }

    private Properties getConnInfo(Map<String, Object> config){
        Properties connInfo = new Properties();
        int len = "connection.".length();
        for (Map.Entry<String, Object> entry : config.entrySet()) {
            if(entry.getKey().startsWith("connection.")){
                connInfo.put(entry.getKey().substring(len), entry.getValue().toString());
            }
        }
        return connInfo;
    }
}
