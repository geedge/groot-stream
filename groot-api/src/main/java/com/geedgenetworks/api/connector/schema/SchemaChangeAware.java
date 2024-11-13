package com.geedgenetworks.api.connector.schema;

import com.geedgenetworks.api.connector.type.StructType;

public interface SchemaChangeAware {
    void schemaChange(StructType dataType);
}
