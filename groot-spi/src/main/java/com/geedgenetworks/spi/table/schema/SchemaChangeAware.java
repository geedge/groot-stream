package com.geedgenetworks.spi.table.schema;

import com.geedgenetworks.spi.table.type.StructType;

public interface SchemaChangeAware {
    void schemaChange(StructType dataType);
}
