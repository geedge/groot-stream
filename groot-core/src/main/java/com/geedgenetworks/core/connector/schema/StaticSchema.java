package com.geedgenetworks.core.connector.schema;

import com.geedgenetworks.core.types.StructType;

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
