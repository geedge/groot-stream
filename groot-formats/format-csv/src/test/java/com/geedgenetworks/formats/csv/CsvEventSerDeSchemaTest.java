package com.geedgenetworks.formats.csv;

import com.alibaba.fastjson2.JSON;
import com.geedgenetworks.api.connector.serialization.MapDeserialization;
import com.geedgenetworks.api.event.Event;
import com.geedgenetworks.api.factory.DecodingFormatFactory;
import com.geedgenetworks.api.factory.EncodingFormatFactory;
import com.geedgenetworks.api.factory.FactoryUtil;
import com.geedgenetworks.api.factory.ConnectorFactory;
import com.geedgenetworks.api.connector.schema.Schema;
import com.geedgenetworks.api.connector.type.StructType;
import com.geedgenetworks.api.connector.type.Types;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class CsvEventSerDeSchemaTest {

    @Test
    public void testSimpleSerializeDeserialize() throws Exception {
        StructType dataType = Types.parseStructType("int:int,bigint:bigint,double:double,string:string");
        Map<String, String> options = new HashMap<>();
        ConnectorFactory.Context context = new ConnectorFactory.Context(Schema.newSchema(dataType), options, Configuration.fromMap(options));

        // 获取deserialization和serialization
        DeserializationSchema<Event> deserialization = FactoryUtil.discoverDecodingFormatFactory(DecodingFormatFactory.class, "csv")
                .createDecodingFormat(context, context.getConfiguration()).createRuntimeDecoder(dataType);
        SerializationSchema<Event> serialization = FactoryUtil.discoverEncodingFormatFactory(EncodingFormatFactory.class, "csv")
                .createEncodingFormat(context, context.getConfiguration()).createRuntimeEncoder(dataType);

        deserialization.open(null);
        serialization.open(null);

        Map<String, Object> map = new HashMap<>();
        map.put("int", 1);
        map.put("bigint", "2");
        map.put("double", "10.2");
        map.put("string", "utf-8字符串");
        Event row = new Event();
        row.setExtractedFields(map);

        byte[] bytes = serialization.serialize(row);
        System.out.println(map);
        System.out.println(new String(bytes, StandardCharsets.UTF_8));
        Map<String, Object> rst = deserialization.deserialize(bytes).getExtractedFields();
        System.out.println(rst);

        // 反序列成map
        if(deserialization instanceof MapDeserialization){
            MapDeserialization mapDeserialization = (MapDeserialization) deserialization;
            Map<String, Object> rstMap = mapDeserialization.deserializeToMap(bytes);
            System.out.println(rstMap);
        }
    }

    @Test
    public void testEventSerDe() throws Exception {
        Map<String, Object> map = new HashMap<>();
        map.put("int", 1);
        map.put("bigint", 2L);
        map.put("double", 10.2);
        map.put("string", "utf-8字符串");
        map.put("int_array", List.of(1, 2, 3));
        map.put("struct", Map.of("int", 1, "string", "22"));
        Event row = new Event();
        row.setExtractedFields(map);

        StructType dataType = Types.parseStructType("int:int,bigint:bigint,double:double,string:string,int_array:array<int>,struct:struct<int:int,string:string>");
        CsvEventDeserializationSchema deser = new CsvEventDeserializationSchema(dataType, CsvFormatFactory.getCsvSchema(dataType, Configuration.fromMap(new HashMap<>())),true);
        CsvEventSerializationSchema ser = new CsvEventSerializationSchema(dataType, CsvFormatFactory.getCsvSchema(dataType, Configuration.fromMap(new HashMap<>())));
        deser.open(null);
        ser.open(null);

        byte[] bytes = ser.serialize(row);
        Event rst = deser.deserialize(bytes);
        Map<String, Object> rstMap = rst.getExtractedFields();
        System.out.println(new String(bytes, StandardCharsets.UTF_8));
        System.out.println(JSON.toJSONString(map));
        System.out.println(JSON.toJSONString(rstMap));
        assertTrue(objEquals(map, rstMap, true), "\n" + JSON.toJSONString(map) + "\n" + JSON.toJSONString(rstMap));
    }

    @Test
    public void testSerializeDeserialize() throws Exception {
        StructType dataType = Types.parseStructType("int:int,bigint:bigint,double:double,string:string,int_array:array<int>,struct:struct<int:int,string:string>");
        Map<String, String> options = new HashMap<>();
        ConnectorFactory.Context context = new ConnectorFactory.Context(Schema.newSchema(dataType), options, Configuration.fromMap(options));

        DeserializationSchema<Event> deserialization = FactoryUtil.discoverDecodingFormatFactory(DecodingFormatFactory.class, "csv")
                .createDecodingFormat(context, context.getConfiguration()).createRuntimeDecoder(dataType);
        SerializationSchema<Event> serialization = FactoryUtil.discoverEncodingFormatFactory(EncodingFormatFactory.class, "csv")
                .createEncodingFormat(context, context.getConfiguration()).createRuntimeEncoder(dataType);

        deserialization.open(null);
        serialization.open(null);

        Map<String, Object> map = new HashMap<>();
        map.put("int", 1);
        map.put("bigint", "2");
        map.put("double", "10.2");
        map.put("string", "utf-8字符串");
        map.put("int_array", Arrays.asList(1 , "2", 3));
        map.put("struct", Map.of("int", "1", "string", 22));
        Event row = new Event();
        row.setExtractedFields(map);

        byte[] bytes = serialization.serialize(row);
        System.out.println(map);
        System.out.println(new String(bytes, StandardCharsets.UTF_8));
        Map<String, Object> rst = deserialization.deserialize(bytes).getExtractedFields();
        System.out.println(rst);

        System.out.println(StringUtils.repeat('*', 60));

        map = new HashMap<>();
        row = new Event();
        map.put("int", 1);
        map.put("double", "10.2");
        map.put("int_array", Arrays.asList(1 , null, null));
        map.put("struct", Map.of( "string", 22));
        row.setExtractedFields(map);

        bytes = serialization.serialize(row);
        System.out.println(map);
        System.out.println(new String(bytes, StandardCharsets.UTF_8));
        rst = deserialization.deserialize(bytes).getExtractedFields();
        System.out.println(rst);


        System.out.println(StringUtils.repeat('*', 60));

        map = new HashMap<>();
        row = new Event();
        row.setExtractedFields(map);

        bytes = serialization.serialize(row);
        System.out.println(map);
        System.out.println(new String(bytes, StandardCharsets.UTF_8));
        rst = deserialization.deserialize(bytes).getExtractedFields();
        System.out.println(rst);

        System.out.println(StringUtils.repeat('*', 60));

        map = new HashMap<>();
        row = new Event();
        map.put("int", 1);
        map.put("bigint", "2");
        map.put("double", "10.2");
        map.put("string", "utf-8字符串");
        map.put("int_array", List.of(1 , "2", 3));
        map.put("struct", Map.of("int", "1", "string", 22));
        row.setExtractedFields(map);

        bytes = serialization.serialize(row);
        System.out.println(map);
        System.out.println(new String(bytes, StandardCharsets.UTF_8));
        rst = deserialization.deserialize(bytes).getExtractedFields();
        System.out.println(rst);
    }


    @Test
    public void testNullableFieldSerializeDeserialize() throws Exception {
        StructType dataType = Types.parseStructType("int:int,bigint:bigint,double:double,string:string,int_array:array<int>,struct:struct<int:int,string:string>");
        Map<String, String> options = new HashMap<>();
        options.put(CsvFormatOptions.NULL_LITERAL.key(), "null");
        options.put(CsvFormatOptions.IGNORE_PARSE_ERRORS.key(), "true");
        ConnectorFactory.Context context = new ConnectorFactory.Context(Schema.newSchema(dataType), options, Configuration.fromMap(options));

        DeserializationSchema<Event> deserialization = FactoryUtil.discoverDecodingFormatFactory(DecodingFormatFactory.class, "csv")
                .createDecodingFormat(context, context.getConfiguration()).createRuntimeDecoder(dataType);
        SerializationSchema<Event> serialization = FactoryUtil.discoverEncodingFormatFactory(EncodingFormatFactory.class, "csv")
                .createEncodingFormat(context, context.getConfiguration()).createRuntimeEncoder(dataType);

        deserialization.open(null);
        serialization.open(null);

        Map<String, Object> map = new HashMap<>();
        map.put("int", 1);
        map.put("bigint", "2");
        map.put("double", "10.2");
        map.put("string", "utf-8字符串");
        map.put("int_array", Arrays.asList(1 , "2", 3));
        map.put("struct", Map.of("int", "1", "string", 22));
        Event row = new Event();
        row.setExtractedFields(map);

        byte[] bytes = serialization.serialize(row);
        System.out.println(map);
        System.out.println(new String(bytes, StandardCharsets.UTF_8));
        Map<String, Object> rst = deserialization.deserialize(bytes).getExtractedFields();
        System.out.println(rst);

        System.out.println(StringUtils.repeat('*', 60));

        map = new HashMap<>();
        row = new Event();
        map.put("int", 1);
        map.put("double", "10.2");
        map.put("int_array", Arrays.asList(1 , null, null));
        map.put("struct", Map.of( "string", 22));
        row.setExtractedFields(map);

        bytes = serialization.serialize(row);
        System.out.println(map);
        System.out.println(new String(bytes, StandardCharsets.UTF_8));
        rst = deserialization.deserialize(bytes).getExtractedFields();
        System.out.println(rst);


        System.out.println(StringUtils.repeat('*', 60));

        map = new HashMap<>();
        row = new Event();
        row.setExtractedFields(map);

        bytes = serialization.serialize(row);
        System.out.println(map);
        System.out.println(new String(bytes, StandardCharsets.UTF_8));
        rst = deserialization.deserialize(bytes).getExtractedFields();
        System.out.println(rst);

        System.out.println(StringUtils.repeat('*', 60));

        map = new HashMap<>();
        row = new Event();
        map.put("int", 1);
        map.put("bigint", "2");
        map.put("double", "10.2");
        map.put("string", "utf-8字符串");
        map.put("int_array", List.of(1 , "2", 3));
        map.put("struct", Map.of("int", "1", "string", 22));
        row.setExtractedFields(map);

        bytes = serialization.serialize(row);
        System.out.println(map);
        System.out.println(new String(bytes, StandardCharsets.UTF_8));
        rst = deserialization.deserialize(bytes).getExtractedFields();
        System.out.println(rst);
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