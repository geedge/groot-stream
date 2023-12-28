package com.geedgenetworks.core.types;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

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

    @Test
    void testParserBaseType() {
        assertEquals(new IntegerType(), Types.parseDataType("INT"));
        assertEquals(new LongType(), Types.parseDataType("biGint"));
        assertEquals(Types.STRING, Types.parseDataType("STRing"));
        assertEquals(Types.FLOAT, Types.parseDataType("float"));
        assertEquals(new DoubleType(), Types.parseDataType("double"));
        assertEquals(new BinaryType(), Types.parseDataType("binary"));

        assertThrows(UnsupportedOperationException.class, () -> Types.parseDataType(""));
        assertThrows(UnsupportedOperationException.class, () -> Types.parseDataType("Long"));
    }

    @Test
    void testParserStructType() {
        DataType dataType = Types.parseDataType("struct < id: int, name : string>");

        assertTrue(dataType instanceof StructType);


        assertThrows(IllegalArgumentException.class, () -> Types.parseDataType("struct < id: int, id: string>"));

        assertThrows(IllegalArgumentException.class, () -> Types.parseSchemaFromJson("[\n" +
                "  {\n" +
                "    \"name\": \"field1\",\n" +
                "    \"type\": \"struct<id: int, id : string>\"\n" +
                "  }\n" +
                "]"));

        StructType structType = Types.parseSchemaFromJson("[\n" +
                "  {\n" +
                "    \"name\": \"field1\",\n" +
                "    \"type\": \"struct<id: int, name : string>\"\n" +
                "  }\n" +
                "]");


        final Map<String, StructType.StructField> map = Arrays.stream(structType.fields)
                .collect(Collectors.toMap(f -> f.name, f -> f));
        assertTrue(map.size() == 1 && map.containsKey("field1"));

        StructType.StructField field1 = map.get("field1");
        assertTrue(field1.dataType instanceof StructType);

        assertEquals(field1.dataType, dataType);
        assertNotEquals(dataType, Types.parseDataType("struct < id: int, name1 : string>"));
        assertNotEquals(dataType, Types.parseDataType("struct < id: int, name : int>"));
    }

}