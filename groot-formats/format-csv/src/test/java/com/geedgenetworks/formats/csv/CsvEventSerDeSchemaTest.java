package com.geedgenetworks.formats.csv;

import com.geedgenetworks.spi.table.connector.MapDeserialization;
import com.geedgenetworks.spi.table.event.Event;
import com.geedgenetworks.spi.table.factory.DecodingFormatFactory;
import com.geedgenetworks.spi.table.factory.EncodingFormatFactory;
import com.geedgenetworks.spi.table.factory.FactoryUtil;
import com.geedgenetworks.spi.table.factory.TableFactory;
import com.geedgenetworks.spi.table.schema.Schema;
import com.geedgenetworks.spi.table.type.StructType;
import com.geedgenetworks.spi.table.type.Types;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class CsvEventSerDeSchemaTest {

    @Test
    public void testSimpleSerializeDeserialize() throws Exception {
        StructType dataType = Types.parseStructType("int:int,bigint:bigint,double:double,string:string");
        Map<String, String> options = new HashMap<>();
        TableFactory.Context context = new TableFactory.Context(Schema.newSchema(dataType), options, Configuration.fromMap(options));

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
    public void testSerializeDeserialize() throws Exception {
        StructType dataType = Types.parseStructType("int:int,bigint:bigint,double:double,string:string,int_array:array<int>,struct:struct<int:int,string:string>");
        Map<String, String> options = new HashMap<>();
        TableFactory.Context context = new TableFactory.Context(Schema.newSchema(dataType), options, Configuration.fromMap(options));

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
        TableFactory.Context context = new TableFactory.Context(Schema.newSchema(dataType), options, Configuration.fromMap(options));

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

}