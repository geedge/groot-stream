package com.geedgenetworks.formats.json;

import com.alibaba.fastjson2.JSONException;
import com.alibaba.fastjson2.JSONReader;
import com.geedgenetworks.spi.table.type.*;
import com.geedgenetworks.spi.table.type.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class JsonToMapDataConverter implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(JsonToMapDataConverter.class);
    private final StructType dataType;
    private final boolean ignoreParseErrors;
    final Map<String, ValueConverter> filedConverters;

    public JsonToMapDataConverter(StructType dataType, boolean ignoreParseErrors) {
        this.dataType = dataType;
        this.ignoreParseErrors = ignoreParseErrors;
        this.filedConverters = Arrays.stream(dataType.fields).collect(Collectors.toMap(f -> f.name, f -> this.makeConverter(f.dataType)));
    }

    public Map<String, Object> convert(String message){
        try (JSONReader reader = JSONReader.of(message)) {
            if (!reader.nextIfMatch('{')) {
                throw new JSONException("object not start with {");
            }

            Map<String, Object> obj = new HashMap<>();

            while (!reader.nextIfMatch('}') ){
                String fieldName = reader.readFieldName();
                ValueConverter converter = filedConverters.get(fieldName);
                if(converter != null){
                    Object rst = converter.convert(reader);
                    obj.put(fieldName, rst);
                }else{
                    reader.skipValue();
                }
            }

            return obj;
        } catch (Exception e) {

            LOG.error(String.format("JSON Parse Errors:%s", message), e);
            if(ignoreParseErrors){
                return null;
            }

            throw new UnsupportedOperationException("Unsupported type or invalid data format:" + message);
        }
    }

    private ValueConverter makeConverter(DataType dataType) {
        if (dataType instanceof StringType) {
            return this::convertToString;
        }

        if (dataType instanceof IntegerType) {
            return this::convertToInteger;
        }

        if (dataType instanceof LongType) {
            return this::convertToLong;
        }

        if (dataType instanceof FloatType) {
            return this::convertToFloat;
        }

        if (dataType instanceof DoubleType) {
            return this::convertToDouble;
        }

        if (dataType instanceof StructType) {
            final Map<String, ValueConverter> filedConverterMap = Arrays.stream(((StructType) dataType).fields).collect(Collectors.toMap(f -> f.name, f -> this.makeConverter(f.dataType)));
            return reader -> {
                if(reader.nextIfNull()){
                    return null;
                }

                Map<String, Object> obj = new HashMap<>();
                // 字符串，当成json对象解析
                if (reader.isString()) {
                    String str = reader.readString();

                    try (JSONReader r = JSONReader.of(str)) {
                        if (!r.nextIfMatch('{')) {
                            return obj;
                        }

                        while (!r.nextIfMatch('}') ){
                            String fieldName = r.readFieldName();
                            ValueConverter converter = filedConverterMap.get(fieldName);
                            if(converter != null){
                                obj.put(fieldName, converter.convert(r));
                            }else{
                                r.skipValue();
                            }
                        }

                        return obj;
                    }
                }

                if (!reader.nextIfMatch('{')) {
                    return obj;
                }

                while (!reader.nextIfMatch('}') ){
                    String fieldName = reader.readFieldName();
                    ValueConverter converter = filedConverterMap.get(fieldName);
                    //System.out.println(fieldName);
                    if(converter != null){
                        obj.put(fieldName, converter.convert(reader));
                    }else{
                        reader.skipValue();
                    }
                }

                reader.nextIfMatch(',');

                return obj;
            };
        }

        if (dataType instanceof ArrayType) {
            ValueConverter converter = this.makeConverter(((ArrayType) dataType).elementType);
            return reader -> {
                if(reader.nextIfNull()){
                    return null;
                }

                // 字符串，当成json数组解析
                if (reader.isString()) {
                    String str = reader.readString();

                    try (JSONReader r = JSONReader.of(str)) {
                        if (!r.nextIfMatch('[')) {
                            return null;
                        }

                        List<Object> objs = new ArrayList<>();
                        while (!r.nextIfMatch(']')){
                            objs.add(converter.convert(r));
                            r.nextIfMatch(',');
                        }
                        return objs;
                    }
                }

                // 数组格式
                if (reader.nextIfMatch('[')) {
                    List<Object> objs = new ArrayList<>();
                    while (!reader.nextIfMatch(']')){
                        objs.add(converter.convert(reader));
                        reader.nextIfMatch(',');
                    }
                    reader.nextIfMatch(',');

                    return objs;
                }

                throw new UnsupportedOperationException("can not convert to array");
            };
        }

        throw new UnsupportedOperationException("unsupported dataType: " + dataType);
    }

    private Object convertToString(JSONReader reader) throws Exception {
        return reader.readString();
    }

    private Object convertToInteger(JSONReader reader) throws Exception {
        return reader.readInt32();
    }

    private Object convertToLong(JSONReader reader) throws Exception {
        return reader.readInt64();
    }

    private Object convertToFloat(JSONReader reader) throws Exception {
        return reader.readFloat();
    }

    private Object convertToDouble(JSONReader reader) throws Exception {
        return reader.readDouble();
    }

    @FunctionalInterface
    public interface ValueConverter extends Serializable {
        Object convert(JSONReader reader) throws Exception;
    }
}
