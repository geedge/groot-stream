package com.geedgenetworks.core.connector.schema;

import com.geedgenetworks.core.types.StructType;

public interface SchemaChangeAware {
    void schemaChange(StructType dataType);
}
