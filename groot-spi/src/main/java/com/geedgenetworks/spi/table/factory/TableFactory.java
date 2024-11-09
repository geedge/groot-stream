package com.geedgenetworks.spi.table.factory;

import com.geedgenetworks.spi.table.schema.Schema;
import com.geedgenetworks.spi.table.type.StructType;
import org.apache.flink.configuration.Configuration;

import java.util.Map;

public interface TableFactory extends Factory {

    public static class Context {
        private final Schema schema;
        private final Map<String, String> options;
        private final Configuration configuration;

        public Context(Schema schema,  Map<String, String> options, Configuration configuration) {
            this.schema = schema;
            this.options = options;
            this.configuration = configuration;
        }

        public Schema getSchema() {
            return schema;
        }

        public StructType getPhysicalDataType() {
            if(schema == null){
                return null;
            }else{
                if(schema.isDynamic()){
                    throw new UnsupportedOperationException("DynamicSchema");
                }
                return schema.getDataType();
            }
        }

        public StructType getDataType() {
            if(schema == null){
                return null;
            }else{
                if(schema.isDynamic()){
                    throw new UnsupportedOperationException("DynamicSchema");
                }
                return schema.getDataType();
            }
        }

        public Map<String, String> getOptions() {
            return options;
        }

        public Configuration getConfiguration() {
            return configuration;
        }
    }
}
