package com.geedgenetworks.core.connector.schema;

import com.geedgenetworks.core.connector.schema.utils.DynamicSchemaManager;
import com.geedgenetworks.core.types.StructType;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;

import static org.apache.flink.util.Preconditions.checkArgument;

public abstract class DynamicSchema implements Schema {
    protected SchemaParser.Parser parser;
    protected StructType dataType;
    private String contentMd5;
    protected final long intervalMs;

    public DynamicSchema(SchemaParser.Parser parser, long intervalMs) {
        this.parser = parser;
        this.intervalMs = intervalMs;
    }

    public abstract String getCacheKey();
    protected abstract String getDataTypeContent();

    @Override
    public StructType getDataType() {
        return dataType;
    }

    protected boolean parseDataType(String _content){
        checkArgument(StringUtils.isNotBlank(_content), "DataType is null");
        String _contentMd5 = computeMd5(_content);
        if(_contentMd5.equals(contentMd5)){
            return false;
        }

        StructType type;
        if(dataType == null){
            type = parser.parser(_content);
            contentMd5 = _contentMd5;
            dataType = type;
            return true;
        }

        type = parser.parser(_content);
        if(dataType.equals(type)){
            return false;
        }else{
            contentMd5 = _contentMd5;
            dataType = type;
            return true;
        }
    }

    // 更新并返回更新后的dataType, 如果没有更新返回null
    public StructType updateDataType(){
        String content = getDataTypeContent();
        if(StringUtils.isBlank(content)){
            return null;
        }
        if(parseDataType(content)){
            return dataType;
        }
        return null;
    }

    final public void registerSchemaChangeAware(SchemaChangeAware aware){
        DynamicSchemaManager.registerSchemaChangeAware(this, aware);
    }

    final public void unregisterSchemaChangeAware(SchemaChangeAware aware){
        DynamicSchemaManager.unregisterSchemaChangeAware(this, aware);
    }

    String computeMd5(String text){
        return DigestUtils.md5Hex(text);
    }

    public long getIntervalMs() {
        return intervalMs;
    }

    @Override
    final public boolean isDynamic() {
        return true;
    }
}
