package com.geedgenetworks.connectors.clickhouse.sink;

import com.alibaba.fastjson2.JSON;

import com.geedgenetworks.core.metrics.InternalMetrics;
import com.geedgenetworks.spi.table.event.Event;
import com.geedgenetworks.spi.table.schema.Schema;
import com.geedgenetworks.spi.table.schema.SchemaChangeAware;
import com.geedgenetworks.spi.table.type.StructType;
import com.github.housepower.data.Block;
import org.apache.flink.configuration.Configuration;

import java.util.*;
import java.util.stream.Collectors;

public class EventBatchIntervalClickHouseSink extends AbstractBatchIntervalClickHouseSink<Event> implements SchemaChangeAware {
    private transient InternalMetrics internalMetrics;
    private Schema schema;
    private Set<String> disabledFields;
    private String simpleName;

    public EventBatchIntervalClickHouseSink(Schema schema, int batchSize, int batchByteSize, long batchIntervalMs, String host, String table, Properties connInfo) {
        super(batchSize, batchByteSize, batchIntervalMs, host, table, connInfo);
        this.schema = schema;
    }

    @Override
    protected void onInit(Configuration parameters) throws Exception {
        super.onInit(parameters);
        initMetric();
        simpleName = this.getClass().getSimpleName() + "_" + getRuntimeContext().getIndexOfThisSubtask();
        if(schema != null){
            updateDisabledFields(schema.getDataType());
            if(schema.isDynamic()){
                Schema.registerSchemaChangeAware(schema, this);
            }
        }
    }

    private void initMetric() {
        internalMetrics = new InternalMetrics(getRuntimeContext());
    }

    @Override
    protected void incInRecords(long n) {
        internalMetrics.incrementInEvents(n);
    }

    @Override
    protected void incOutRecords(long n) {
        internalMetrics.incrementOutEvents(n);
    }

    @Override
    protected void incOutBytes(long n) {
        internalMetrics.incrementOutBytes(n);
    }

    @Override
    protected void incErrorRecords(long n) {
        internalMetrics.incrementErrorEvents(n);
    }

    @Override
    protected int addBatch(Block batch, Event event) throws Exception {
        int writeSize = 0;
        Map<String, Object> map = event.getExtractedFields();
        String columnName;
        Object value;
        for (int i = 0; i < columnNames.length; i++) {
            columnName = columnNames[i];
            if(disabledFields != null && disabledFields.contains(columnName)){
                value = columnDefaultValues[i];
                batch.setObject(i, value); // 默认值不用转换
                writeSize += columnDefaultSizes[i];
                continue;
            }

            value = map.get(columnName);

            if (value == null) {
                value = columnDefaultValues[i];
                batch.setObject(i, value); // 默认值不用转换
                writeSize += columnDefaultSizes[i];
            } else {
                // int columnIdx = batch.paramIdx2ColumnIdx(i);
                // batch.setObject(columnIdx, convertToCkDataType(columnTypes[i], value));
                // batch.setObject(i, convertToCkDataType(dataType, value));
                try {
                    writeSizeHelper.size = 0;
                    batch.setObject(i, columnConverters[i].convert(value, writeSizeHelper));
                    writeSize += writeSizeHelper.size;
                } catch (Exception e) {
                    throw new RuntimeException(columnNames[i] + "列转换值出错:" + value + ", event data:" + JSON.toJSONString(map), e);
                }
            }
        }

        return writeSize;
    }

    @Override
    public void schemaChange(StructType dataType) {
        if(schema != null && schema.isDynamic()){
            updateDisabledFields(dataType);
        }
    }

    @Override
    protected void onClose() throws Exception {
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
