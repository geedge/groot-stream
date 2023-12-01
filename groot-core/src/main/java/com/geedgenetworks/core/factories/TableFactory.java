package com.geedgenetworks.core.factories;

import com.geedgenetworks.core.types.StructType;
import org.apache.flink.configuration.Configuration;

import java.util.Map;

public interface TableFactory extends Factory {

    public static class Context {
        private final StructType schema;
        private final StructType physicalDataType;
        private final Map<String, String> options;
        private final Configuration configuration;

        public Context(StructType schema, StructType physicalDataType, Map<String, String> options, Configuration configuration) {
            this.schema = schema;
            this.physicalDataType = physicalDataType;
            this.options = options;
            this.configuration = configuration;
        }

        public StructType getSchema() {
            return schema;
        }

        public StructType getPhysicalDataType() {
            return physicalDataType;
        }

        public Map<String, String> getOptions() {
            return options;
        }

        public Configuration getConfiguration() {
            return configuration;
        }
    }
}
