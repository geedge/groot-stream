package com.geedgenetworks.core.types;

public class ArrayType extends DataType {
    public DataType elementType;

    public ArrayType(DataType elementType) {
        this.elementType = elementType;
    }

    @Override
    public String simpleString() {
        return "array";
    }
}
