package com.geedgenetworks.core.connector.schema;

import com.geedgenetworks.core.connector.schema.utils.DynamicSchemaManager;
import com.geedgenetworks.core.types.StructType;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkArgument;

public interface Schema extends Serializable {
    StructType getDataType();

    boolean isDynamic();

    public static Schema newSchema(StructType dataType){
        return new StaticSchema(dataType);
    }

    public static Schema newHttpDynamicSchema(String url){
        HttpDynamicSchema dynamicSchema = new HttpDynamicSchema(url, SchemaParser.PARSER_AVRO, 1000 * 60 * 30);
        checkArgument(dynamicSchema.getDataType() != null);
        return dynamicSchema;
    }

    public static void registerSchemaChangeAware(Schema schema, SchemaChangeAware aware){
        Preconditions.checkArgument(schema.isDynamic());
        DynamicSchemaManager.registerSchemaChangeAware((DynamicSchema)schema, aware);
    }

    public static void unregisterSchemaChangeAware(Schema schema, SchemaChangeAware aware){
        Preconditions.checkArgument(schema.isDynamic());
        DynamicSchemaManager.unregisterSchemaChangeAware((DynamicSchema)schema, aware);
    }
}
