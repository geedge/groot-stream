package com.geedgenetworks.sink.clickhouse;

import com.geedgenetworks.pojo.Event;
import com.github.housepower.data.Block;
import com.github.housepower.data.IDataType;
import com.github.housepower.data.type.complex.DataTypeArray;
import com.github.housepower.jdbc.ClickHouseArray;
import org.apache.flink.configuration.Configuration;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class EventBatchIntervalClickHouseSink extends AbstractBatchIntervalClickHouseSink<Event>{
    transient private ValueConverter[] columnConverters;

    public EventBatchIntervalClickHouseSink(int batchSize, long batchIntervalMs, String[] urls, String table, Properties connInfo) {
        super(batchSize, batchIntervalMs, urls, table, connInfo);
    }

    @Override
    void onInit(Configuration parameters) throws Exception {
        super.onInit(parameters);
        columnConverters = Arrays.stream(columnTypes).map(this::makeConverter).toArray(ValueConverter[]::new);
    }

    @Override
    boolean addBatch(Block batch, Event event) throws Exception {
        Map<String, Object> map = event.getExtractedFields();
        Object value;
        for (int i = 0; i < columnNames.length; i++) {
            value = map.get(columnNames[i]);

            IDataType<?, ?> dataType = columnTypes[i];
            if (dataType instanceof DataTypeArray) {
                if(value != null){
                    value = convertArray(value, ((DataTypeArray)dataType).getElemDataType());
                }
            }

            if(value == null){
                value = columnDefaultValues[i];
            }
            //int columnIdx = batch.paramIdx2ColumnIdx(i);
            //batch.setObject(columnIdx, convertToCkDataType(columnTypes[i], value));
            //batch.setObject(i, convertToCkDataType(dataType, value));
            batch.setObject(i, columnConverters[i].convert(value));
        }

        return true;
    }

    private Object convertArray(Object value, IDataType<?, ?> eleDataType) throws Exception{
        if(value instanceof ClickHouseArray){
            return value;
        }
        if(value instanceof List){
            List list = (List) value;
            if(list.size() == 0){
                return null;
            }
            Object[] elements = new Object[list.size()];
            for (int i = 0; i < elements.length; i++) {
                elements[i] = convertToCkDataType(eleDataType, list.get(i));
            }
            return new ClickHouseArray(eleDataType, elements);
        }

        return value;
    }
}
