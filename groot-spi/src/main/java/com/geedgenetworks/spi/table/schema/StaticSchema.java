package com.geedgenetworks.spi.table.schema;


import com.geedgenetworks.spi.table.type.StructType;

public class StaticSchema implements Schema{
    private final StructType dataType;

    public StaticSchema(StructType dataType) {
        this.dataType = dataType;
    }

    @Override
    public StructType getDataType() {
        return dataType;
    }

    @Override
    final public boolean isDynamic() {
        return false;
    }
}
