package com.geedgenetworks.core.types;

public class TypesTest {

    public static void main(String[] args) {
        TypesTest test = new TypesTest();
        test.parseDataType();
    }

    public void parseDataType(){
        String ddl = "struct< id: int, obj: struct <id:int, arr: array < int >, name:string>, name : string>";
        StructType dataType = (StructType)Types.parseDataType(ddl);
        System.out.println(dataType);
        System.out.println(dataType.treeString());

        System.out.println();
        ddl = "struct< id: int, objs: array<struct<id:int, name:string>>, arr: array<int>, name : string>";
        dataType = (StructType)Types.parseDataType(ddl);
        System.out.println(dataType);
        System.out.println(dataType.treeString());
    }

}