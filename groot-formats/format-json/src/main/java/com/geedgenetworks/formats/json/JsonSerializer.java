package com.geedgenetworks.formats.json;

import com.alibaba.fastjson2.JSONWriter;
import com.geedgenetworks.api.connector.type.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JsonSerializer implements Serializable{

    private final StructType dataType;
    private final ValueWriter valueWriter;

    public JsonSerializer(StructType dataType) {
        this.dataType = dataType;
        this.valueWriter = makeWriter(dataType);
    }

    public byte[] serialize(Map<String, Object> data){
        try (JSONWriter writer = JSONWriter.ofUTF8()) {
            if (data == null) {
                writer.writeNull();
            } else {
                valueWriter.write(writer, data);
            }
            return writer.getBytes();
        }
    }

    private ValueWriter makeWriter(DataType dataType) {
        if (dataType instanceof StringType) {
            return JsonSerializer::writeString;
        }

        if (dataType instanceof IntegerType) {
            return JsonSerializer::writeInt;
        }

        if (dataType instanceof LongType) {
            return JsonSerializer::writeLong;
        }

        if (dataType instanceof FloatType) {
            return JsonSerializer::writeFloat;
        }

        if (dataType instanceof DoubleType) {
            return JsonSerializer::writeDouble;
        }

        if (dataType instanceof StructType) {
            final Map<String, ValueWriter> fieldWriters = Arrays.stream(((StructType) dataType).fields).collect(Collectors.toMap(f -> f.name, f -> this.makeWriter(f.dataType)));
            return (writer, obj) -> {
                writeObject(writer, obj, fieldWriters);
            };
        }

        if (dataType instanceof ArrayType) {
            final ValueWriter elementWriter = this.makeWriter(((ArrayType) dataType).elementType);
            return (writer, obj) -> {
                writeArray(writer, obj, elementWriter);
            };
        }

        throw new UnsupportedOperationException("unsupported dataType: " + dataType);
    }

    static void writeString(JSONWriter writer, Object obj) {
        writer.writeString(obj.toString());
    }

    static void writeInt(JSONWriter writer, Object obj){
        if(obj instanceof Number){
            writer.writeInt32(((Number) obj).intValue());
        } else if(obj instanceof String){
            writer.writeInt32(Integer.parseInt((String) obj));
        } else {
            throw new IllegalArgumentException(String.format("can not convert %s to int", obj));
        }
    }

    static void writeLong(JSONWriter writer, Object obj) {
        if(obj instanceof Number){
            writer.writeInt64(((Number) obj).longValue());
        } else if(obj instanceof String){
            writer.writeInt64(Long.parseLong((String) obj));
        } else {
            throw new IllegalArgumentException(String.format("can not convert %s to long", obj));
        }
    }

    static void writeFloat(JSONWriter writer, Object obj) {
        if(obj instanceof Number){
            writer.writeFloat(((Number) obj).floatValue());
        } else if(obj instanceof String){
            writer.writeFloat(Float.parseFloat((String) obj));
        } else {
            throw new IllegalArgumentException(String.format("can not convert %s to float", obj));
        }
    }

    static void writeDouble(JSONWriter writer, Object obj){
        if(obj instanceof Number){
            writer.writeDouble(((Number) obj).doubleValue());
        } else if(obj instanceof String){
            writer.writeDouble(Double.parseDouble((String) obj));
        } else {
            throw new IllegalArgumentException(String.format("can not convert %s to double", obj));
        }
    }

    static void writeObject(JSONWriter writer, Object obj, Map<String, ValueWriter> fieldWriters){
        if(obj instanceof Map){
            Map<String, Object> map = (Map<String, Object>) obj;
            writer.startObject();

            String key;
            Object value;
            ValueWriter valueWriter;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                key = entry.getKey();
                /*if (key.startsWith("__")) {
                    continue;
                }*/
                value = entry.getValue();
                if(value == null){
                    continue;
                }
                valueWriter = fieldWriters.get(key);
                if(valueWriter != null){
                    writer.writeName(key);
                    writer.writeColon();
                    valueWriter.write(writer, value);
                }
            }

            writer.endObject();
        } else {
            throw new IllegalArgumentException(String.format("can not convert %s to map", obj));
        }
    }

    static void writeArray(JSONWriter writer, Object obj, ValueWriter elementWriter){
        if(obj instanceof List){
            List<Object> list = (List<Object>) obj;
            writer.startArray();

            Object element;
            for (int i = 0; i < list.size(); i++) {
                if (i != 0) {
                    writer.writeComma();
                }

                element = list.get(i);
                if (element == null) {
                    writer.writeNull();
                    continue;
                }

                elementWriter.write(writer, element);
            }

            writer.endArray();
        } else {
            throw new IllegalArgumentException(String.format("can not convert %s to list", obj));
        }
    }

    @FunctionalInterface
    public interface ValueWriter extends Serializable {
        void write(JSONWriter writer, Object obj);
    }
}
