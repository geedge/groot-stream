package com.geedgenetworks.connectors.clickhouse.sink;

import com.alibaba.fastjson2.JSON;
import com.geedgenetworks.common.Event;

import com.geedgenetworks.core.connector.schema.Schema;
import com.geedgenetworks.core.connector.schema.SchemaChangeAware;
import com.geedgenetworks.core.types.StructType;
import com.github.housepower.data.Block;
import org.apache.flink.configuration.Configuration;

import java.util.*;
import java.util.stream.Collectors;

public class EventBatchIntervalClickHouseSink extends AbstractBatchIntervalClickHouseSink<Event> implements SchemaChangeAware {
    private Schema schema;
    private Set<String> disabledFields;
    private String simpleName;

    public EventBatchIntervalClickHouseSink(Schema schema, int batchSize, long batchIntervalMs, String host, String table, Properties connInfo) {
        super(batchSize, batchIntervalMs, host, table, connInfo);
        this.schema = schema;
    }

    @Override
    void onInit(Configuration parameters) throws Exception {
        super.onInit(parameters);
        simpleName = this.getClass().getSimpleName() + "_" + getRuntimeContext().getIndexOfThisSubtask();
        if(schema != null){
            updateDisabledFields(schema.getDataType());
            if(schema.isDynamic()){
                Schema.registerSchemaChangeAware(schema, this);
            }
        }
    }

    @Override
    boolean addBatch(Block batch, Event event) throws Exception {
        Map<String, Object> map = event.getExtractedFields();
        String columnName;
        Object value;
        for (int i = 0; i < columnNames.length; i++) {
            columnName = columnNames[i];
            if(disabledFields != null && disabledFields.contains(columnName)){
                value = columnDefaultValues[i];
                batch.setObject(i, value); // 默认值不用转换
                continue;
            }

            value = map.get(columnName);

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
                    throw new RuntimeException(columnNames[i] + "列转换值出错:" + value + ", event data:" + JSON.toJSONString(map), e);
                }
            }
        }

        return true;
    }

    @Override
    public void schemaChange(StructType dataType) {
        if(schema != null && schema.isDynamic()){
            updateDisabledFields(dataType);
        }
    }

    @Override
    void onClose() throws Exception {
        super.onClose();
        if(schema != null && schema.isDynamic()){
            Schema.unregisterSchemaChangeAware(schema, this);
        }
    }

    private void updateDisabledFields(StructType dataType){
        Set<String> disabledFields = new HashSet<>();
        Set<String> names = Arrays.stream(dataType.fields).map(x -> x.name).collect(Collectors.toSet());
        for (String columnName : this.columnNames) {
            if(!names.contains(columnName)){
                disabledFields.add(columnName);
            }
        }
        LOG.info("disabledFields: {}", disabledFields);
        this.disabledFields = disabledFields.isEmpty()?null: disabledFields;
    }

    @Override
    public String toString() {
        return simpleName;
    }
}
