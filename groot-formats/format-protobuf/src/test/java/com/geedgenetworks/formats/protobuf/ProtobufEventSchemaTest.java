package com.geedgenetworks.formats.protobuf;

import com.alibaba.fastjson2.JSON;
import com.geedgenetworks.shaded.com.google.protobuf.ByteString;
import com.geedgenetworks.shaded.com.google.protobuf.Descriptors;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.flink.util.Preconditions;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.junit.jupiter.api.Test;
import com.geedgenetworks.formats.protobuf.SchemaConverters.MessageConverter;

import java.io.FileInputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.*;

/**
 * protoc --descriptor_set_out=proto3_types.desc --java_out=./ proto3_types.proto
 * protoc --descriptor_set_out=session_record_test.desc session_record_test.proto
 *
 */
public class ProtobufEventSchemaTest {

    public static class InputDatas{
        Proto3TypesProtos.Proto3Types msg;
        Proto3TypesProtos.StructMessage subMsg1;
        Proto3TypesProtos.StructMessage subMsg2;
        Map<String, Object> map;
        Map<String, Object> subMap1;
        Map<String, Object> subMap2;
    }

    public static InputDatas geneInputDatas(){
        ThreadLocalRandom random = ThreadLocalRandom.current();
        Proto3TypesProtos.Proto3Types.Builder msgBuilder = Proto3TypesProtos.Proto3Types.newBuilder();
        Map<String, Object> map = new HashMap<>();
        Proto3TypesProtos.StructMessage.Builder subMsgBuilder1 = Proto3TypesProtos.StructMessage.newBuilder();
        Proto3TypesProtos.StructMessage.Builder subMsgBuilder2 = Proto3TypesProtos.StructMessage.newBuilder();
        Map<String, Object> subMap1 = new HashMap<>();
        Map<String, Object> subMap2 = new HashMap<>();

        long int64 = random.nextLong(1, Long.MAX_VALUE);
        msgBuilder.setInt64(int64);
        map.put("int64", int64);

        int int32 = random.nextInt(1, Integer.MAX_VALUE);
        msgBuilder.setInt32(int32);
        map.put("int32", int32);

        String text = "ut8字符串";
        msgBuilder.setText(text);
        map.put("text", text);

        byte[] bytes = new byte[]{1, 2, 3, 4, 5};
        msgBuilder.setBytes(ByteString.copyFrom(bytes));
        map.put("bytes", bytes);

        int enum_val = 1;
        msgBuilder.setEnumValValue(enum_val);
        map.put("enum_val", enum_val);

        // subMsg start
        long id = random.nextLong(1, Long.MAX_VALUE);
        subMsgBuilder1.setId(id);
        subMap1.put("id", id);

        String name = "ut8字符串1";
        subMsgBuilder1.setName(name);
        subMap1.put("name", name);

        int age = random.nextInt(1, Integer.MAX_VALUE);
        subMsgBuilder1.setAge(age);
        subMap1.put("age", age);

        double score = random.nextDouble(1, Integer.MAX_VALUE);
        subMsgBuilder1.setScore(score);
        subMap1.put("score", score);

        long optional_id = random.nextLong(1, Long.MAX_VALUE);
        subMsgBuilder1.setOptionalId(optional_id);
        subMap1.put("optional_id", optional_id);

        int optional_age = random.nextInt(1, Integer.MAX_VALUE);
        subMsgBuilder1.setOptionalAge(optional_age);
        subMap1.put("optional_age", optional_age);

        id = random.nextLong(1, Long.MAX_VALUE);
        subMsgBuilder2.setId(id);
        subMap2.put("id", id);

        name = "ut8字符串1";
        subMsgBuilder2.setName(name);
        subMap2.put("name", name);

        age = random.nextInt(1, Integer.MAX_VALUE);
        subMsgBuilder2.setAge(age);
        subMap2.put("age", age);

        score = random.nextDouble(1, Integer.MAX_VALUE);
        subMsgBuilder2.setScore(score);
        subMap2.put("score", score);

        optional_id = random.nextLong(1, Long.MAX_VALUE);
        subMsgBuilder2.setOptionalId(optional_id);
        subMap2.put("optional_id", optional_id);

        optional_age = random.nextInt(1, Integer.MAX_VALUE);
        subMsgBuilder2.setOptionalAge(optional_age);
        subMap2.put("optional_age", optional_age);

        Proto3TypesProtos.StructMessage subMsg1 = subMsgBuilder1.build();
        Proto3TypesProtos.StructMessage subMsg2 = subMsgBuilder2.build();
        // subMsg end

        msgBuilder.setMessage(subMsg1);
        map.put("message", subMap1);

        long optional_int64 = random.nextLong(1, Long.MAX_VALUE);
        msgBuilder.setOptionalInt64(optional_int64);
        map.put("optional_int64", optional_int64);

        int optional_int32 = random.nextInt(1, Integer.MAX_VALUE);
        msgBuilder.setOptionalInt32(optional_int32);
        map.put("optional_int32", optional_int32);

        String optional_text = "ut8字符串";
        msgBuilder.setOptionalText(optional_text);
        map.put("optional_text", optional_text);

        byte[] optional_bytes = new byte[]{1, 2, 3, 4, 5};
        msgBuilder.setOptionalBytes(ByteString.copyFrom(optional_bytes));
        map.put("optional_bytes", optional_bytes);

        int optional_enum_val = 1;
        msgBuilder.setOptionalEnumValValue(optional_enum_val);
        map.put("optional_enum_val", optional_enum_val);

        msgBuilder.setOptionalMessage(subMsg2);
        map.put("optional_message", subMap2);

        List<Long> repeated_int64 = Arrays.asList(1L, 3L, 5L);
        msgBuilder.addAllRepeatedInt64(repeated_int64);
        map.put("repeated_int64", repeated_int64);

        List<Integer> repeated_int32 = Arrays.asList(1, 3, 5);
        msgBuilder.addAllRepeatedInt32(repeated_int32);
        map.put("repeated_int32", repeated_int32);

        msgBuilder.addAllRepeatedMessage(Arrays.asList(subMsg1, subMsg2));
        map.put("repeated_message", Arrays.asList(subMap1, subMap2));

        InputDatas datas = new InputDatas();
        datas.msg = msgBuilder.build();
        datas.subMsg1 = subMsg1;
        datas.subMsg2 = subMsg2;
        datas.map = map;
        datas.subMap1 = subMap1;
        datas.subMap2 = subMap2;
        return datas;
    }

    public static InputDatas geneInputDatasDefaultValue(){
        ThreadLocalRandom random = ThreadLocalRandom.current();
        Proto3TypesProtos.Proto3Types.Builder msgBuilder = Proto3TypesProtos.Proto3Types.newBuilder();
        Map<String, Object> map = new HashMap<>();
        Proto3TypesProtos.StructMessage.Builder subMsgBuilder1 = Proto3TypesProtos.StructMessage.newBuilder();
        Proto3TypesProtos.StructMessage.Builder subMsgBuilder2 = Proto3TypesProtos.StructMessage.newBuilder();
        Map<String, Object> subMap1 = new HashMap<>();
        Map<String, Object> subMap2 = new HashMap<>();

        long int64 = 0;
        msgBuilder.setInt64(int64);
        map.put("int64", int64);

        int int32 = 0;
        msgBuilder.setInt32(int32);
        map.put("int32", int32);

        String text = "";
        msgBuilder.setText(text);
        map.put("text", text);

        byte[] bytes = new byte[]{};
        msgBuilder.setBytes(ByteString.copyFrom(bytes));
        map.put("bytes", bytes);

        int enum_val = 0;
        msgBuilder.setEnumValValue(enum_val);
        map.put("enum_val", enum_val);

        // subMsg start
        long id = 0;
        subMsgBuilder1.setId(id);
        subMap1.put("id", id);

        String name = "";
        subMsgBuilder1.setName(name);
        subMap1.put("name", name);

        int age = 0;
        subMsgBuilder1.setAge(age);
        subMap1.put("age", age);

        double score = 0;
        subMsgBuilder1.setScore(score);
        subMap1.put("score", score);

        long optional_id = 0;
        subMsgBuilder1.setOptionalId(optional_id);
        subMap1.put("optional_id", optional_id);

        int optional_age = 0;
        /*subMsgBuilder1.setOptionalAge(optional_age);
        subMap1.put("optional_age", optional_age);*/

        id = 0;
        subMsgBuilder2.setId(id);
        subMap2.put("id", id);

        name = "";
        subMsgBuilder2.setName(name);
        subMap2.put("name", name);

        age = 0;
        subMsgBuilder2.setAge(age);
        subMap2.put("age", age);

        score = 0;
        subMsgBuilder2.setScore(score);
        subMap2.put("score", score);

        optional_id = 0;
        subMsgBuilder2.setOptionalId(optional_id);
        subMap2.put("optional_id", optional_id);

        optional_age = 0;
        subMsgBuilder2.setOptionalAge(optional_age);
        subMap2.put("optional_age", optional_age);

        Proto3TypesProtos.StructMessage subMsg1 = subMsgBuilder1.build();
        Proto3TypesProtos.StructMessage subMsg2 = subMsgBuilder2.build();
        // subMsg end

        msgBuilder.setMessage(subMsg1);
        map.put("message", subMap1);

        long optional_int64 = 0;
        msgBuilder.setOptionalInt64(optional_int64);
        map.put("optional_int64", optional_int64);

        int optional_int32 = 0;
        msgBuilder.setOptionalInt32(optional_int32);
        map.put("optional_int32", optional_int32);

        String optional_text = "";
        msgBuilder.setOptionalText(optional_text);
        map.put("optional_text", optional_text);

        byte[] optional_bytes = new byte[]{};
        msgBuilder.setOptionalBytes(ByteString.copyFrom(optional_bytes));
        map.put("optional_bytes", optional_bytes);

        int optional_enum_val = 0;
        msgBuilder.setOptionalEnumValValue(optional_enum_val);
        map.put("optional_enum_val", optional_enum_val);

        msgBuilder.setOptionalMessage(subMsg2);
        map.put("optional_message", subMap2);

        List<Long> repeated_int64 = Arrays.asList();
        msgBuilder.addAllRepeatedInt64(repeated_int64);
        map.put("repeated_int64", repeated_int64);

        List<Integer> repeated_int32 = Arrays.asList();
        msgBuilder.addAllRepeatedInt32(repeated_int32);
        map.put("repeated_int32", repeated_int32);

        msgBuilder.addAllRepeatedMessage(Arrays.asList());
        map.put("repeated_message", Arrays.asList());

        InputDatas datas = new InputDatas();
        datas.msg = msgBuilder.build();
        datas.subMsg1 = subMsg1;
        datas.subMsg2 = subMsg2;
        datas.map = map;
        datas.subMap1 = subMap1;
        datas.subMap2 = subMap2;
        return datas;
    }

    public static InputDatas geneInputDatasUsePartialField(){
        ThreadLocalRandom random = ThreadLocalRandom.current();
        Proto3TypesProtos.Proto3Types.Builder msgBuilder = Proto3TypesProtos.Proto3Types.newBuilder();
        Map<String, Object> map = new HashMap<>();
        Proto3TypesProtos.StructMessage.Builder subMsgBuilder1 = Proto3TypesProtos.StructMessage.newBuilder();
        Proto3TypesProtos.StructMessage.Builder subMsgBuilder2 = Proto3TypesProtos.StructMessage.newBuilder();
        Map<String, Object> subMap1 = new HashMap<>();
        Map<String, Object> subMap2 = new HashMap<>();

        /*long int64 = random.nextLong(1, Long.MAX_VALUE);
        msgBuilder.setInt64(int64);
        map.put("int64", int64);*/

        int int32 = random.nextInt(1, Integer.MAX_VALUE);
        msgBuilder.setInt32(int32);
        map.put("int32", int32);

        String text = "ut8字符串";
        msgBuilder.setText(text);
        map.put("text", text);

        /*byte[] bytes = new byte[]{1, 2, 3, 4, 5};
        msgBuilder.setBytes(ByteString.copyFrom(bytes));
        map.put("bytes", bytes);*/

        /*int enum_val = 1;
        msgBuilder.setEnumValValue(enum_val);
        map.put("enum_val", enum_val);*/

        // subMsg start
        long id = random.nextLong(1, Long.MAX_VALUE);
        subMsgBuilder1.setId(id);
        subMap1.put("id", id);

        String name = "ut8字符串1";
        /*subMsgBuilder1.setName(name);
        subMap1.put("name", name);*/

        int age = random.nextInt(1, Integer.MAX_VALUE);
        subMsgBuilder1.setAge(age);
        subMap1.put("age", age);

        double score = random.nextDouble(1, Integer.MAX_VALUE);
        /*subMsgBuilder1.setScore(score);
        subMap1.put("score", score);*/

        long optional_id = random.nextLong(1, Long.MAX_VALUE);
        subMsgBuilder1.setOptionalId(optional_id);
        subMap1.put("optional_id", optional_id);

        int optional_age = random.nextInt(1, Integer.MAX_VALUE);
        /*subMsgBuilder1.setOptionalAge(optional_age);
        subMap1.put("optional_age", optional_age);*/

        id = random.nextLong(1, Long.MAX_VALUE);
        /*subMsgBuilder2.setId(id);
        subMap2.put("id", id);*/

        name = "ut8字符串1";
        subMsgBuilder2.setName(name);
        subMap2.put("name", name);

        age = random.nextInt(1, Integer.MAX_VALUE);
        /*subMsgBuilder2.setAge(age);
        subMap2.put("age", age);*/

        score = random.nextDouble(1, Integer.MAX_VALUE);
        subMsgBuilder2.setScore(score);
        subMap2.put("score", score);

        optional_id = random.nextLong(1, Long.MAX_VALUE);
        /*subMsgBuilder2.setOptionalId(optional_id);
        subMap2.put("optional_id", optional_id);*/

        optional_age = random.nextInt(1, Integer.MAX_VALUE);
        subMsgBuilder2.setOptionalAge(optional_age);
        subMap2.put("optional_age", optional_age);

        Proto3TypesProtos.StructMessage subMsg1 = subMsgBuilder1.build();
        Proto3TypesProtos.StructMessage subMsg2 = subMsgBuilder2.build();
        // subMsg end

        /*msgBuilder.setMessage(subMsg1);
        map.put("message", subMap1);*/

        long optional_int64 = random.nextLong(1, Long.MAX_VALUE);
        msgBuilder.setOptionalInt64(optional_int64);
        map.put("optional_int64", optional_int64);

        /*int optional_int32 = random.nextInt(1, Integer.MAX_VALUE);
        msgBuilder.setOptionalInt32(optional_int32);
        map.put("optional_int32", optional_int32);*/

        String optional_text = "ut8字符串";
        msgBuilder.setOptionalText(optional_text);
        map.put("optional_text", optional_text);

        /*byte[] optional_bytes = new byte[]{1, 2, 3, 4, 5};
        msgBuilder.setOptionalBytes(ByteString.copyFrom(optional_bytes));
        map.put("optional_bytes", optional_bytes);*/

        int optional_enum_val = 1;
        msgBuilder.setOptionalEnumValValue(optional_enum_val);
        map.put("optional_enum_val", optional_enum_val);

        msgBuilder.setOptionalMessage(subMsg2);
        map.put("optional_message", subMap2);

        /*List<Long> repeated_int64 = Arrays.asList(1L, 3L, 5L);
        msgBuilder.addAllRepeatedInt64(repeated_int64);
        map.put("repeated_int64", repeated_int64);*/

        List<Integer> repeated_int32 = Arrays.asList(1, 3, 5);
        msgBuilder.addAllRepeatedInt32(repeated_int32);
        map.put("repeated_int32", repeated_int32);

        msgBuilder.addAllRepeatedMessage(Arrays.asList(subMsg1, subMsg2));
        map.put("repeated_message", Arrays.asList(subMap1, subMap2));

        InputDatas datas = new InputDatas();
        datas.msg = msgBuilder.build();
        datas.subMsg1 = subMsg1;
        datas.subMsg2 = subMsg2;
        datas.map = map;
        datas.subMap1 = subMap1;
        datas.subMap2 = subMap2;
        return datas;
    }

    @Test
    public void testSerializeAndDeserialize() throws Exception{
        String path = getClass().getResource("/proto3_types.desc").getPath();
        Descriptors.Descriptor descriptor = ProtobufUtils.buildDescriptor(ProtobufUtils.readDescriptorFileContent(path),"Proto3Types");
        InputDatas inputDatas = geneInputDatas();

        byte[] bytesSerByApi = inputDatas.msg.toByteArray();

        ProtobufSerializer serializer = new ProtobufSerializer(descriptor);
        byte[] bytesSer = serializer.serialize(inputDatas.map);

        System.out.println(String.format("built-in ser bytes size:%d\nmy ser bytes size:%d", bytesSerByApi.length, bytesSer.length));
        assertArrayEquals(bytesSerByApi, bytesSer);

        MessageConverter messageConverter = new MessageConverter(descriptor, SchemaConverters.toStructType(descriptor), false);
        Map<String, Object> rstMap = messageConverter.converter(bytesSer);

        assertTrue(objEquals(inputDatas.map, rstMap, false), () -> "\n" + inputDatas.map.toString() + "\n" + rstMap.toString());
        System.out.println(inputDatas.map.toString());
        System.out.println(rstMap.toString());
        System.out.println(JSON.toJSONString(inputDatas.map));
        System.out.println(JSON.toJSONString(rstMap));

        System.out.println(JSON.toJSONString(inputDatas.map).equals(JSON.toJSONString(rstMap)));
    }

    @Test
    public void testSerializeAndDeserializeDefaultValue() throws Exception{
        String path = getClass().getResource("/proto3_types.desc").getPath();
        Descriptors.Descriptor descriptor = ProtobufUtils.buildDescriptor(ProtobufUtils.readDescriptorFileContent(path),"Proto3Types");
        InputDatas inputDatas = geneInputDatasDefaultValue();

        byte[] bytesSerByApi = inputDatas.msg.toByteArray();

        ProtobufSerializer serializer = new ProtobufSerializer(descriptor);
        byte[] bytesSer = serializer.serialize(inputDatas.map);

        System.out.println(String.format("built-in ser bytes size:%d\nmy ser bytes size:%d", bytesSerByApi.length, bytesSer.length));
        assertArrayEquals(bytesSerByApi, bytesSer);

        MessageConverter messageConverter = new MessageConverter(descriptor, SchemaConverters.toStructType(descriptor), false);
        Map<String, Object> rstMap = messageConverter.converter(bytesSer);
        messageConverter = new MessageConverter(descriptor, SchemaConverters.toStructType(descriptor), true);
        Map<String, Object> rstMapEmitDefaultValue = messageConverter.converter(bytesSer);

        // message不是null就输出, 数组长度大于0才输出, optional设置值就输出, optional bytes长度为0也输出
        System.out.println(inputDatas.map.toString());
        System.out.println(rstMap.toString());
        System.out.println(rstMapEmitDefaultValue.toString());
        System.out.println(JSON.toJSONString(inputDatas.map));
        System.out.println(JSON.toJSONString(rstMap));
        System.out.println(JSON.toJSONString(rstMapEmitDefaultValue));

        System.out.println(JSON.toJSONString(inputDatas.map).equals(JSON.toJSONString(rstMap)));
    }

    @Test
    public void testSerializeAndDeserializeUsePartialField() throws Exception{
        String path = getClass().getResource("/proto3_types.desc").getPath();
        Descriptors.Descriptor descriptor = ProtobufUtils.buildDescriptor(ProtobufUtils.readDescriptorFileContent(path),"Proto3Types");
        InputDatas inputDatas = geneInputDatasUsePartialField();

        byte[] bytesSerByApi = inputDatas.msg.toByteArray();

        ProtobufSerializer serializer = new ProtobufSerializer(descriptor);
        byte[] bytesSer = serializer.serialize(inputDatas.map);
        System.out.println(Base64.getEncoder().encodeToString(bytesSer));

        System.out.println(String.format("built-in ser bytes size:%d\nmy ser bytes size:%d", bytesSerByApi.length, bytesSer.length));
        assertArrayEquals(bytesSerByApi, bytesSer);

        MessageConverter messageConverter = new MessageConverter(descriptor, SchemaConverters.toStructType(descriptor), false);
        Map<String, Object> rstMap = messageConverter.converter(bytesSer);

        assertTrue(objEquals(inputDatas.map, rstMap, false), () -> "\n" + inputDatas.map.toString() + "\n" + rstMap.toString());
        System.out.println(inputDatas.map.toString());
        System.out.println(rstMap.toString());
        System.out.println(JSON.toJSONString(inputDatas.map));
        System.out.println(JSON.toJSONString(rstMap));

        System.out.println(JSON.toJSONString(inputDatas.map).equals(JSON.toJSONString(rstMap)));
    }

    @Test
    public void testSerializeAndDeserializeSessionRecord() throws Exception{
        String path = getClass().getResource("/session_record_test.desc").getPath();
        Descriptors.Descriptor descriptor = ProtobufUtils.buildDescriptor(ProtobufUtils.readDescriptorFileContent(path),"SessionRecord");
        String json = "{\"recv_time\": 1704350600, \"log_id\": 185826449998479360, \"decoded_as\": \"BASE\", \"session_id\": 290502878495441820, \"start_timestamp_ms\": 1704350566378, \"end_timestamp_ms\": 1704350570816, \"duration_ms\": 4438, \"tcp_handshake_latency_ms\": 1105, \"ingestion_time\": 1704350600, \"processing_time\": 1704350600, \"device_id\": \"21426003\", \"out_link_id\": 65535, \"in_link_id\": 65535, \"device_tag\": \"{\\\"tags\\\":[{\\\"tag\\\":\\\"data_center\\\",\\\"value\\\":\\\"center-xxg-9140\\\"},{\\\"tag\\\":\\\"device_group\\\",\\\"value\\\":\\\"group-xxg-9140\\\"}]}\", \"data_center\": \"center-xxg-9140\", \"device_group\": \"group-xxg-9140\", \"sled_ip\": \"192.168.40.81\", \"address_type\": 4, \"vsys_id\": 1, \"t_vsys_id\": 1, \"flags\": 24592, \"flags_identify_info\": \"[1,1,2]\", \"statistics_rule_list\": [406583], \"client_ip\": \"192.56.151.80\", \"client_port\": 62241, \"client_os_desc\": \"Windows\", \"client_geolocation\": \"\\u7f8e\\u56fd.Unknown.Unknown..\", \"server_ip\": \"192.56.222.93\", \"server_port\": 14454, \"server_os_desc\": \"Linux\", \"server_geolocation\": \"\\u7f8e\\u56fd.Unknown.Unknown..\", \"ip_protocol\": \"tcp\", \"decoded_path\": \"ETHERNET.IPv4.TCP\", \"sent_pkts\": 4, \"received_pkts\": 5, \"sent_bytes\": 246, \"received_bytes\": 1809, \"tcp_rtt_ms\": 128, \"tcp_client_isn\": 568305009, \"tcp_server_isn\": 4027331180, \"in_src_mac\": \"a2:fa:dc:56:c7:b3\", \"out_src_mac\": \"48:73:97:96:38:20\", \"in_dest_mac\": \"48:73:97:96:38:20\", \"out_dest_mac\": \"a2:fa:dc:56:c7:b3\"}";
        Map<String, Object> map = JSON.parseObject(json);

        ProtobufSerializer serializer = new ProtobufSerializer(descriptor);
        byte[] bytesSer = serializer.serialize(map);
        System.out.println(Base64.getEncoder().encodeToString(bytesSer));

        System.out.println(String.format("my ser bytes size:%d",  bytesSer.length));

        MessageConverter messageConverter = new MessageConverter(descriptor, SchemaConverters.toStructType(descriptor), false);
        Map<String, Object> rstMap = messageConverter.converter(bytesSer);

        assertTrue(objEquals(map, rstMap, true), () -> "\n" + JSON.toJSONString(map) + "\n" + JSON.toJSONString(rstMap));
        System.out.println(map.toString());
        System.out.println(rstMap.toString());
        System.out.println(JSON.toJSONString(new TreeMap<>(map)));
        System.out.println(JSON.toJSONString(new TreeMap<>(rstMap)));

        System.out.println(JSON.toJSONString(new TreeMap<>(map)).equals(JSON.toJSONString(new TreeMap<>(rstMap))));
    }


    public static void main(String[] args) throws Exception{
        ProtobufEventSchemaTest test = new ProtobufEventSchemaTest();
        String path = test.getClass().getResource("/session_record_test.desc").getPath();
        Descriptors.Descriptor descriptor = ProtobufUtils.buildDescriptor(ProtobufUtils.readDescriptorFileContent(path),"SessionRecord");
        MessageConverter messageConverter = new MessageConverter(descriptor, SchemaConverters.toStructType(descriptor), false);
        ProtobufSerializer serializer = new ProtobufSerializer(descriptor);

        FileInputStream inputStream = new FileInputStream( test.getClass().getResource("/format_protobuf_test_data.json").getPath());
        LineIterator lines = IOUtils.lineIterator(inputStream, "utf-8");
        int count = 0;
        long jsonBytesTotalSize = 0;
        long protoBytesTotalSize = 0;
        long jsonBytesMinSize = Long.MAX_VALUE;
        long protoBytesMinSize = Long.MAX_VALUE;
        long jsonBytesMaxSize = 0;
        long protoBytesMaxSize = 0;
        long totalFieldCount = 0;
        long minFieldCount = Long.MAX_VALUE;
        long maxFieldCount = 0;

        CompressionType[] compressionTypes = new CompressionType[]{
                CompressionType.NONE,  CompressionType.SNAPPY,   CompressionType.LZ4, CompressionType.GZIP, CompressionType.ZSTD
        };
        long[][] compressionBytesSize = new long[compressionTypes.length][6];
        for (int i = 0; i < compressionBytesSize.length; i++) {
            compressionBytesSize[i][0] = 0;
            compressionBytesSize[i][1] = 0;
            compressionBytesSize[i][2] = Long.MAX_VALUE;
            compressionBytesSize[i][3] = Long.MAX_VALUE;
            compressionBytesSize[i][4] = 0;
            compressionBytesSize[i][5] = 0;
        }

        while (lines.hasNext()){
            String line = lines.next().trim();
            if(line.isEmpty()){
                continue;
            }

            Map<String, Object> map = JSON.parseObject(line);
            int fieldCount = map.size();
            byte[] bytesProto = serializer.serialize(map);
            byte[] bytesJson = JSON.toJSONString(map).getBytes(StandardCharsets.UTF_8);
            jsonBytesTotalSize += bytesJson.length;
            protoBytesTotalSize += bytesProto.length;
            jsonBytesMinSize = Math.min(jsonBytesMinSize, bytesJson.length);
            protoBytesMinSize = Math.min(protoBytesMinSize, bytesProto.length);
            jsonBytesMaxSize = Math.max(jsonBytesMaxSize, bytesJson.length);
            protoBytesMaxSize = Math.max(protoBytesMaxSize, bytesProto.length);
            totalFieldCount += fieldCount;
            minFieldCount = Math.min(minFieldCount, fieldCount);
            maxFieldCount = Math.max(maxFieldCount, fieldCount);

            Map<String, Object> rstMap = messageConverter.converter(bytesProto);
            Preconditions.checkArgument(test.objEquals(map, rstMap, true), "\n" + JSON.toJSONString(new TreeMap<>(map)) + "\n" + JSON.toJSONString(new TreeMap<>(rstMap)));
            Preconditions.checkArgument(JSON.toJSONString(new TreeMap<>(map)).equals(JSON.toJSONString(new TreeMap<>(rstMap))), "\n" + JSON.toJSONString(new TreeMap<>(map)) + "\n" + JSON.toJSONString(new TreeMap<>(rstMap)));
            count++;

            for (int i = 0; i < compressionTypes.length; i++) {
                CompressionType compressionType = compressionTypes[i];
                ByteBufferOutputStream bufferStream = new ByteBufferOutputStream(1024 * 16);
                OutputStream outputStream = compressionType.wrapForOutput(bufferStream, (byte) 2);
                outputStream.write(bytesJson);
                outputStream.close();
                int jsonCompressSize = bufferStream.position();

                bufferStream = new ByteBufferOutputStream(1024 * 16);
                outputStream = compressionType.wrapForOutput(bufferStream, (byte) 2);
                outputStream.write(bytesProto);
                outputStream.close();
                int protoCompressSize = bufferStream.position();

                compressionBytesSize[i][0] += jsonCompressSize;
                compressionBytesSize[i][1] += protoCompressSize;
                compressionBytesSize[i][2] = Math.min(compressionBytesSize[i][2], jsonCompressSize);
                compressionBytesSize[i][3] = Math.min(compressionBytesSize[i][3], protoCompressSize);
                compressionBytesSize[i][4] = Math.max(compressionBytesSize[i][4], jsonCompressSize);
                compressionBytesSize[i][5] = Math.max(compressionBytesSize[i][5], protoCompressSize);
            }

        }
        System.out.println(String.format("count:%d, avgFieldCount:%d, minFieldCount:%d, maxFieldCount:%d, jsonBytesAvgSize:%d, protoBytesAvgSize:%d, jsonBytesMinSize:%d, protoBytesMinSize:%d, jsonBytesMaxSize:%d, protoBytesMaxSize:%d",
                count, totalFieldCount/count, minFieldCount, maxFieldCount, jsonBytesTotalSize/count, protoBytesTotalSize/count,
                jsonBytesMinSize, protoBytesMinSize, jsonBytesMaxSize, protoBytesMaxSize));
        for (int i = 0; i < compressionTypes.length; i++) {
            CompressionType compressionType = compressionTypes[i];
            System.out.println(String.format("compression(%s): count:%d, jsonBytesAvgSize:%d, protoBytesAvgSize:%d, avgRatio:%.2f, jsonBytesMinSize:%d, protoBytesMinSize:%d, minRatio:%.2f, jsonBytesMaxSize:%d, protoBytesMaxSize:%d, maxRatio:%.2f",
                    compressionType, count, compressionBytesSize[i][0]/count, compressionBytesSize[i][1]/count, (((double)compressionBytesSize[i][1])/count)/(compressionBytesSize[i][0]/count),
                    compressionBytesSize[i][2], compressionBytesSize[i][3], ((double)compressionBytesSize[i][3])/(compressionBytesSize[i][2]),
                    compressionBytesSize[i][4], compressionBytesSize[i][5], ((double)compressionBytesSize[i][5])/(compressionBytesSize[i][4])));
        }
    }

    @Test
    public void testArrayInstance() throws Exception{
        Object bytes = new byte[]{1, 2, 3, 4, 5};
        Object ints = new int[]{1, 2, 3, 4, 5};

        System.out.println(bytes.getClass().isArray());
        System.out.println(bytes instanceof byte[]);
        System.out.println(bytes instanceof int[]);
        System.out.println(ints.getClass().isArray());
        System.out.println(ints instanceof byte[]);
        System.out.println(ints instanceof int[]);
    }

    private boolean objEquals(Object value1, Object value2, boolean numConvert){
        if(value1 == null){
            if(value1 != value2){
                return false;
            }
        }else if(value2 == null){
            return false;
        }else if(value1 instanceof Map){
            if(!mapEquals((Map<String, Object>) value1, (Map<String, Object>) value2, numConvert)){
                return false;
            }
        }else if(value1 instanceof List){
            if(!listEquals((List< Object>) value1, (List< Object>) value2, numConvert)){
                return false;
            }
        }else if(value1 instanceof byte[]){
            if(!Arrays.equals((byte[]) value1, (byte[]) value2)){
                return false;
            }
        }
        else{
            if(value1.getClass() != value2.getClass() || !value1.equals(value2)){
                if(numConvert && value1 instanceof Number && value2 instanceof Number && ((Number) value1).longValue() == ((Number) value2).longValue()){

                }else{
                    return false;
                }
            }
        }
        return true;
    }
    private boolean mapEquals(Map<String, Object> map1, Map<String, Object> map2, boolean numConvert){
        if(map1.size() != map2.size()){
            return false;
        }

        for (Map.Entry<String, Object> entry : map1.entrySet()) {
            Object value1 = entry.getValue();
            Object value2 = map2.get(entry.getKey());
            if(!objEquals(value1, value2, numConvert)){
                return false;
            }
        }

        return true;
    }

    private boolean listEquals(List< Object> list1, List< Object> list2, boolean numConvert){
        if(list1.size() != list2.size()){
            return false;
        }

        for (int i = 0; i < list1.size(); i++) {
            if(!objEquals(list1.get(i), list2.get(i), numConvert)){
                return false;
            }
        }

        return true;
    }
}
