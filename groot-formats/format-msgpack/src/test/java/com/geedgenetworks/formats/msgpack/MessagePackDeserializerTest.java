package com.geedgenetworks.formats.msgpack;

import com.alibaba.fastjson2.JSON;
import com.geedgenetworks.api.connector.type.StructType;
import com.geedgenetworks.api.connector.type.Types;
import org.junit.jupiter.api.Test;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.value.MapValue;
import org.msgpack.value.ValueFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class MessagePackDeserializerTest {
    @Test
    public void testDeserSimpleData() throws Exception{
        ValueFactory.MapBuilder map = ValueFactory.newMapBuilder();
        map.put(ValueFactory.newString("uint8"), ValueFactory.newInteger(123));
        map.put(ValueFactory.newString("uint16"), ValueFactory.newInteger(512));
        map.put(ValueFactory.newString("uint32"), ValueFactory.newInteger(33554432));
        map.put(ValueFactory.newString("uint64"), ValueFactory.newInteger(17179869184L));
        map.put(ValueFactory.newString("int8"), ValueFactory.newInteger(-123));
        map.put(ValueFactory.newString("int16"), ValueFactory.newInteger(-512));
        map.put(ValueFactory.newString("int32"), ValueFactory.newInteger(-33554432));
        map.put(ValueFactory.newString("int64"), ValueFactory.newInteger(-17179869184L));
        map.put(ValueFactory.newString("null"), ValueFactory.newNil());

        map.put(ValueFactory.newString("double"), ValueFactory.newFloat(123.2));

        map.put(ValueFactory.newString("bool_true"), ValueFactory.newBoolean(true));
        map.put(ValueFactory.newString("bool_false"), ValueFactory.newBoolean(false));

        map.put(ValueFactory.newString("str"), ValueFactory.newString("ut8字符串"));

        map.put(ValueFactory.newString("binary"), ValueFactory.newBinary("ut8字符串".getBytes(StandardCharsets.UTF_8)));

        map.put(ValueFactory.newString("int32_array"), ValueFactory.newArray(ValueFactory.newInteger(123), ValueFactory.newInteger(512), ValueFactory.newNil(), ValueFactory.newInteger(33554432)));
        map.put(ValueFactory.newString("str_array"), ValueFactory.newArray(ValueFactory.newString("ut8字符串1"), ValueFactory.newNil(), ValueFactory.newString("ut8字符串2")));

        map.put(ValueFactory.newString("obj"), ValueFactory.newMapBuilder()
                .put(ValueFactory.newString("uint8"), ValueFactory.newInteger(123))
                .put(ValueFactory.newString("uint32"), ValueFactory.newInteger(33554432))
                .put(ValueFactory.newString("double"), ValueFactory.newFloat(123.2))
                .put(ValueFactory.newString("str"), ValueFactory.newString("ut8字符串"))
                .build());


        MapValue mapValue = map.build();
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        packer.packValue(mapValue);
        byte[] bytes = packer.toByteArray();
        packer.close();

        MessagePackDeserializer deserializer = new MessagePackDeserializer(null);
        Map<String, Object> rst = deserializer.deserialize(bytes);
        System.out.println(mapValue.toJson());
        System.out.println(JSON.toJSONString(rst));

        assertEquals(rst.get("uint8"), 123);
        assertEquals(rst.get("uint16"), 512);
        assertEquals(rst.get("uint32"), 33554432L);
        assertEquals(rst.get("uint64"), 17179869184L);
        assertEquals(rst.get("int8"), -123);
        assertEquals(rst.get("int16"), -512);
        assertEquals(rst.get("int32"), -33554432);
        assertEquals(rst.get("int64"), -17179869184L);

        assertEquals(rst.get("double"), 123.2);
        assertEquals(rst.get("bool_true"), true);
        assertEquals(rst.get("bool_false"), false);

        assertEquals(rst.get("str"), "ut8字符串");
        assertArrayEquals((byte[]) rst.get("binary"), "ut8字符串".getBytes(StandardCharsets.UTF_8));
        assertArrayEquals(((List<Object>) rst.get("int32_array")).toArray(), new Object[]{123,512,null,33554432L});
        assertArrayEquals(((List<Object>) rst.get("str_array")).toArray(), new Object[]{"ut8字符串1",null,"ut8字符串2"});

        assertEquals(((Map<String, Object>)rst.get("obj")).get("uint8"), 123);
        assertEquals(((Map<String, Object>)rst.get("obj")).get("uint32"), 33554432L    );
        assertEquals(((Map<String, Object>)rst.get("obj")).get("double"), 123.2);
        assertEquals(((Map<String, Object>)rst.get("obj")).get("str"), "ut8字符串");

    }

    @Test
    public void testDeserSimpleDataWithSchema() throws Exception{
        ValueFactory.MapBuilder map = ValueFactory.newMapBuilder();
        map.put(ValueFactory.newString("uint8"), ValueFactory.newInteger(123));
        map.put(ValueFactory.newString("uint16"), ValueFactory.newInteger(512));
        map.put(ValueFactory.newString("uint32"), ValueFactory.newInteger(33554432));
        map.put(ValueFactory.newString("uint64"), ValueFactory.newInteger(17179869184L));
        map.put(ValueFactory.newString("int8"), ValueFactory.newInteger(-123));
        map.put(ValueFactory.newString("int16"), ValueFactory.newInteger(-512));
        map.put(ValueFactory.newString("int32"), ValueFactory.newInteger(-33554432));
        map.put(ValueFactory.newString("int64"), ValueFactory.newInteger(-17179869184L));
        map.put(ValueFactory.newString("null"), ValueFactory.newNil());

        map.put(ValueFactory.newString("double"), ValueFactory.newFloat(123.2));

        map.put(ValueFactory.newString("bool_true"), ValueFactory.newBoolean(true));
        map.put(ValueFactory.newString("bool_false"), ValueFactory.newBoolean(false));

        map.put(ValueFactory.newString("str"), ValueFactory.newString("ut8字符串"));

        map.put(ValueFactory.newString("binary"), ValueFactory.newBinary("ut8字符串".getBytes(StandardCharsets.UTF_8)));

        map.put(ValueFactory.newString("int32_array"), ValueFactory.newArray(ValueFactory.newInteger(123), ValueFactory.newInteger(512), ValueFactory.newNil(), ValueFactory.newInteger(33554432)));
        map.put(ValueFactory.newString("str_array"), ValueFactory.newArray(ValueFactory.newString("ut8字符串1"), ValueFactory.newNil(), ValueFactory.newString("ut8字符串2")));

        map.put(ValueFactory.newString("obj"), ValueFactory.newMapBuilder()
                .put(ValueFactory.newString("uint8"), ValueFactory.newInteger(123))
                .put(ValueFactory.newString("uint32"), ValueFactory.newInteger(33554432))
                .put(ValueFactory.newString("double"), ValueFactory.newFloat(123.2))
                .put(ValueFactory.newString("str"), ValueFactory.newString("ut8字符串"))
                .build());


        MapValue mapValue = map.build();
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        packer.packValue(mapValue);
        byte[] bytes = packer.toByteArray();
        packer.close();

        StructType dataType = Types.parseStructType("struct<uint8: int, uint16: int, uint32: int, uint64: bigint, int8: int, int16: int, int32: int, int64: bigint, double: double," +
                "bool_true: boolean, bool_false: boolean, str: string, binary: binary, int32_array:array<int>, str_array:array<string>, " +
                "obj:struct<uint8: int, uint32: int, double: double, str: string>>");
        MessagePackDeserializer deserializer = new MessagePackDeserializer(dataType);
        Map<String, Object> rst = deserializer.deserialize(bytes);
        System.out.println(mapValue.toJson());
        System.out.println(JSON.toJSONString(rst));

        assertEquals(rst.get("uint8"), 123);
        assertEquals(rst.get("uint16"), 512);
        assertEquals(rst.get("uint32"), 33554432);
        assertEquals(rst.get("uint64"), 17179869184L);
        assertEquals(rst.get("int8"), -123);
        assertEquals(rst.get("int16"), -512);
        assertEquals(rst.get("int32"), -33554432);
        assertEquals(rst.get("int64"), -17179869184L);

        assertEquals(rst.get("double"), 123.2);
        assertEquals(rst.get("bool_true"), true);
        assertEquals(rst.get("bool_false"), false);

        assertEquals(rst.get("str"), "ut8字符串");
        assertArrayEquals((byte[]) rst.get("binary"), "ut8字符串".getBytes(StandardCharsets.UTF_8));
        assertArrayEquals(((List<Object>) rst.get("int32_array")).toArray(), new Object[]{123,512,null,33554432});
        assertArrayEquals(((List<Object>) rst.get("str_array")).toArray(), new Object[]{"ut8字符串1",null,"ut8字符串2"});

        assertEquals(((Map<String, Object>)rst.get("obj")).get("uint8"), 123);
        assertEquals(((Map<String, Object>)rst.get("obj")).get("uint32"), 33554432 );
        assertEquals(((Map<String, Object>)rst.get("obj")).get("double"), 123.2);
        assertEquals(((Map<String, Object>)rst.get("obj")).get("str"), "ut8字符串");

    }

    @Test
    public void testDeserSimpleDataWithSchemaTypeConvert() throws Exception{
        ValueFactory.MapBuilder map = ValueFactory.newMapBuilder();
        map.put(ValueFactory.newString("uint8"), ValueFactory.newString("123"));
        map.put(ValueFactory.newString("uint16"), ValueFactory.newInteger(512));
        map.put(ValueFactory.newString("uint32"), ValueFactory.newString("33554432"));
        map.put(ValueFactory.newString("uint64"), ValueFactory.newString("17179869184"));
        map.put(ValueFactory.newString("int8"), ValueFactory.newInteger(-123));
        map.put(ValueFactory.newString("int16"), ValueFactory.newInteger(-512));
        map.put(ValueFactory.newString("int32"), ValueFactory.newInteger(-33554432));
        map.put(ValueFactory.newString("int64"), ValueFactory.newString("-17179869184"));
        map.put(ValueFactory.newString("null"), ValueFactory.newNil());

        map.put(ValueFactory.newString("double"), ValueFactory.newString("123.2"));

        map.put(ValueFactory.newString("bool_true"), ValueFactory.newBoolean(true));
        map.put(ValueFactory.newString("bool_false"), ValueFactory.newBoolean(false));

        map.put(ValueFactory.newString("str"), ValueFactory.newString("ut8字符串"));

        map.put(ValueFactory.newString("binary"), ValueFactory.newBinary("ut8字符串".getBytes(StandardCharsets.UTF_8)));

        map.put(ValueFactory.newString("int32_array"), ValueFactory.newArray(ValueFactory.newInteger(123), ValueFactory.newString("512"), ValueFactory.newNil(), ValueFactory.newInteger(33554432)));
        map.put(ValueFactory.newString("str_array"), ValueFactory.newArray(ValueFactory.newString("ut8字符串1"), ValueFactory.newNil(), ValueFactory.newString("ut8字符串2")));

        map.put(ValueFactory.newString("obj"), ValueFactory.newMapBuilder()
                .put(ValueFactory.newString("uint8"), ValueFactory.newInteger(123))
                .put(ValueFactory.newString("uint32"), ValueFactory.newString("33554432"))
                .put(ValueFactory.newString("double"), ValueFactory.newString("123.2"))
                .put(ValueFactory.newString("str"), ValueFactory.newString("ut8字符串"))
                .build());


        MapValue mapValue = map.build();
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        packer.packValue(mapValue);
        byte[] bytes = packer.toByteArray();
        packer.close();

        StructType dataType = Types.parseStructType("struct<uint8: int, uint16: string, uint32: int, uint64: bigint, int8: int, int16: string, int32: int, int64: bigint, double: double," +
                "bool_true: int, bool_false: boolean, str: string, binary: binary, int32_array:array<int>, str_array:array<string>, " +
                "obj:struct<uint8: string, uint32: int, double: double, str: binary>>");
        MessagePackDeserializer deserializer = new MessagePackDeserializer(dataType);
        Map<String, Object> rst = deserializer.deserialize(bytes);
        System.out.println(mapValue.toJson());
        System.out.println(JSON.toJSONString(rst));

        assertEquals(rst.get("uint8"), 123);
        assertEquals(rst.get("uint16"), "512");
        assertEquals(rst.get("uint32"), 33554432);
        assertEquals(rst.get("uint64"), 17179869184L);
        assertEquals(rst.get("int8"), -123);
        assertEquals(rst.get("int16"), "-512");
        assertEquals(rst.get("int32"), -33554432);
        assertEquals(rst.get("int64"), -17179869184L);

        assertEquals(rst.get("double"), 123.2);
        assertEquals(rst.get("bool_true"), 1);
        assertEquals(rst.get("bool_false"), false);

        assertEquals(rst.get("str"), "ut8字符串");
        assertArrayEquals((byte[]) rst.get("binary"), "ut8字符串".getBytes(StandardCharsets.UTF_8));
        assertArrayEquals(((List<Object>) rst.get("int32_array")).toArray(), new Object[]{123,512,null,33554432});
        assertArrayEquals(((List<Object>) rst.get("str_array")).toArray(), new Object[]{"ut8字符串1",null,"ut8字符串2"});

        assertEquals(((Map<String, Object>)rst.get("obj")).get("uint8"), "123");
        assertEquals(((Map<String, Object>)rst.get("obj")).get("uint32"), 33554432 );
        assertEquals(((Map<String, Object>)rst.get("obj")).get("double"), 123.2);
        assertArrayEquals((byte[])((Map<String, Object>)rst.get("obj")).get("str"), "ut8字符串".getBytes(StandardCharsets.UTF_8));

    }
}