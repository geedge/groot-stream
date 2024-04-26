package com.geedgenetworks.formats.msgpack;

import com.alibaba.fastjson2.JSON;
import com.geedgenetworks.core.types.StructType;
import com.geedgenetworks.core.types.Types;
import org.junit.jupiter.api.Test;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.value.MapValue;
import org.msgpack.value.ValueFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class MessagePackSerializerTest {

    public static void main(String[] args) throws Exception {
        // '{"log_id": 1, "recv_time":"111", "client_ip":"192.168.0.1"}'
        ValueFactory.MapBuilder map = ValueFactory.newMapBuilder();
        map.put(ValueFactory.newString("log_id"), ValueFactory.newInteger(1));
        map.put(ValueFactory.newString("recv_time"), ValueFactory.newInteger(System.currentTimeMillis() / 1000));
        map.put(ValueFactory.newString("client_ip"), ValueFactory.newString("192.168.0.1"));
        MapValue mapValue = map.build();
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        packer.packValue(mapValue);
        byte[] bytes = packer.toByteArray();
        packer.close();
        String str = Base64.getEncoder().encodeToString(bytes);
        System.out.println(mapValue);
        System.out.println(str);
    }

    @Test
    public void testStringEncodeDecodeReversibility() throws Exception {
        byte[] bytes1 = "一个utf-8字符串".getBytes(StandardCharsets.UTF_8);
        byte[] bytes2 = new byte[256];
        for (int i = 0; i < bytes2.length; i++) {
            bytes2[i] = (byte) i;
        }
        byte[] bytes3 = new byte[128];
        for (int i = 0; i < bytes3.length; i++) {
            bytes3[i] = (byte) i;
        }

        List<byte[]> bytesList = Arrays.asList(bytes1, bytes2, bytes3);
        for (byte[] bytes : bytesList) {
            String str = new String(bytes, StandardCharsets.UTF_8);
            byte[] bytesEncodeDecode = str.getBytes(StandardCharsets.UTF_8);
            System.out.println(str);
            System.out.println(bytes.length + "," + bytesEncodeDecode.length +  "," + Arrays.equals(bytes, bytesEncodeDecode));
            System.out.println("--------");
        }
    }

    @Test
    public void testJsonToString() throws Exception {
        Object[] objs = new Object[]{1, 512, 33554432, 17179869184L,123.2 ,1233333.23, "abc", "ut8字符串"};
        for (Object obj : objs) {
            System.out.println(obj.toString() + " , " + JSON.toJSONString(obj)+ " , " + obj.toString().equals(JSON.toJSONString(obj)));
        }
    }

    @Test
    public void testSerSimpleData() throws Exception{
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
        Map<String, Object> data = deserializer.deserialize(bytes);

        MessagePackSerializer serializer = new MessagePackSerializer(null);
        byte[] bytes2 = serializer.serialize(data);
        Map<String, Object> rst = deserializer.deserialize(bytes2);

        System.out.println(mapValue.toJson());
        System.out.println(JSON.toJSONString(data));
        System.out.println(JSON.toJSONString(rst));

        System.out.println(bytes.length + "," + bytes2.length);

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

        for (int i = 0; i < 10; i++) {
            //System.out.println("###########" + i);
            bytes2 = serializer.serialize(data);
            rst = deserializer.deserialize(bytes2);

            System.out.println(bytes.length + "," + bytes2.length);

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
    }

    @Test
    public void testSerSimpleDataWithSchema() throws Exception{
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
        Map<String, Object> data = deserializer.deserialize(bytes);

        MessagePackSerializer serializer = new MessagePackSerializer(dataType);
        byte[] bytes2 = serializer.serialize(data);
        Map<String, Object> rst = deserializer.deserialize(bytes2);

        String str = new String(bytes2, StandardCharsets.UTF_8);
        byte[] bytes3 = str.getBytes(StandardCharsets.UTF_8);
        System.out.println(bytes2.length + "," + bytes3.length +  "," + Arrays.equals(bytes2, bytes3));

        System.out.println(mapValue.toJson());
        System.out.println(JSON.toJSONString(data));
        System.out.println(JSON.toJSONString(rst));

        System.out.println(bytes.length + "," + bytes2.length);

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

        for (int i = 0; i < 10; i++) {
            //System.out.println("###########" + i);
            bytes2 = serializer.serialize(data);
            rst = deserializer.deserialize(bytes2);

            System.out.println(bytes.length + "," + bytes2.length);

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
    }

    @Test
    public void testSerSimpleDataWithSchemaTypeConvert() throws Exception{
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

        StructType dataType2 = Types.parseStructType("struct<uint8: int, uint16: int, uint32: int, uint64: bigint, int8: int, int16: int, int32: int, int64: bigint, double: double," +
                "bool_true: boolean, bool_false: boolean, str: string, binary: binary, int32_array:array<int>, str_array:array<string>, " +
                "obj:struct<uint8: int, uint32: int, double: double, str: string>>");

        MessagePackDeserializer deserializer = new MessagePackDeserializer(dataType);
        Map<String, Object> data = deserializer.deserialize(bytes);

        MessagePackSerializer serializer = new MessagePackSerializer(dataType2);
        byte[] bytes2 = serializer.serialize(data);
        Map<String, Object> rst = deserializer.deserialize(bytes2);

        System.out.println(mapValue.toJson());
        System.out.println(JSON.toJSONString(data));
        System.out.println(JSON.toJSONString(rst));

        System.out.println(bytes.length + "," + bytes2.length);

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

        for (int i = 0; i < 10; i++) {
            //System.out.println("###########" + i);
            bytes2 = serializer.serialize(data);
            rst = deserializer.deserialize(bytes2);

            System.out.println(bytes.length + "," + bytes2.length);

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
}