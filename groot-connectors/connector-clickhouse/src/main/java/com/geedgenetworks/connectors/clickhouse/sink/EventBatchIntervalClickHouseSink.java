package com.geedgenetworks.connectors.clickhouse.sink;

import com.geedgenetworks.core.pojo.Event;

import com.github.housepower.data.Block;

import java.util.Map;
import java.util.Properties;

public class EventBatchIntervalClickHouseSink extends AbstractBatchIntervalClickHouseSink<Event> {

    public EventBatchIntervalClickHouseSink(
            int batchSize, long batchIntervalMs, String host, String table, Properties connInfo) {
        super(batchSize, batchIntervalMs, host, table, connInfo);
    }

    @Override
    boolean addBatch(Block batch, Event event) throws Exception {
        Map<String, Object> map = event.getExtractedFields();
        Object value;
        for (int i = 0; i < columnNames.length; i++) {
            value = map.get(columnNames[i]);

            if (value == null) {
                value = columnDefaultValues[i];
                batch.setObject(i, value); // 默认值不用转换
            } else {
                // int columnIdx = batch.paramIdx2ColumnIdx(i);
                // batch.setObject(columnIdx, convertToCkDataType(columnTypes[i], value));
                // batch.setObject(i, convertToCkDataType(dataType, value));
                try {
                    batch.setObject(i, columnConverters[i].convert(value));
                } catch (Exception e) {
                    throw new RuntimeException(columnNames[i] + "列转换值出错:" + value + ", event data:" + map, e);
                }
            }
        }

        return true;
    }
}
