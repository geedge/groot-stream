package com.geedgenetworks.core.types;

import java.io.Serializable;

public class StructType extends DataType {
    public final StructField[] fields;

    public StructType(StructField[] fields) {
        this.fields = fields;
    }

    @Override
    public String simpleString() {
        return "struct";
    }

    public static final class StructField implements Serializable {
        public final String name;
        public final DataType dataType;

        public StructField(String name, DataType dataType) {
            this.name = name;
            this.dataType = dataType;
        }
    }

}
