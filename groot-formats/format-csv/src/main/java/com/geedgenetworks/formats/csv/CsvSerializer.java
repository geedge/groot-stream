package com.geedgenetworks.formats.csv;

import com.geedgenetworks.core.types.*;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.*;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CsvSerializer implements Serializable {
    private final StructType dataType;
    private final CsvSchema csvSchema;
    private final ValueConverter[] fieldConverters;
    private final String[] fields;
    private transient CsvMapper csvMapper;
    private transient ObjectNode root;
    private transient ObjectWriter objectWriter;

    public CsvSerializer(StructType dataType, CsvSchema csvSchema) {
        this.dataType = dataType;
        this.csvSchema = csvSchema;
        this.fieldConverters = Arrays.stream(dataType.fields).map(f -> this.makeConverter(f.dataType)).toArray(ValueConverter[]::new);
        this.fields = Arrays.stream(dataType.fields).map(f -> f.name).toArray(String[]::new);
    }

    public byte[] serialize(Map<String, Object> data) throws IOException {
        if (root == null) {
            csvMapper = new CsvMapper();
            root = csvMapper.createObjectNode();
            objectWriter = csvMapper.writer(csvSchema);
        }

        String field;
        Object value;
        for (int i = 0; i < fields.length; i++) {
            field = fields[i];
            value = data.get(field);
            if (value == null) {
                root.set(field, NullNode.getInstance());
            }else{
                root.set(field, fieldConverters[i].convert(value));
            }
        }

        return objectWriter.writeValueAsBytes(root);
    }

    private ValueConverter makeConverter(DataType dataType) {
        if (dataType instanceof StringType) {
            return this::convertString;
        }

        if (dataType instanceof IntegerType) {
            return this::convertInteger;
        }

        if (dataType instanceof LongType) {
            return this::convertLong;
        }

        if (dataType instanceof FloatType) {
            return this::convertFloat;
        }

        if (dataType instanceof DoubleType) {
            return this::convertDouble;
        }

        if (dataType instanceof BooleanType) {
            return this::convertBoolean;
        }

        if (dataType instanceof StructType) {
            final ValueConverter[] fieldConverters = Arrays.stream(((StructType) dataType).fields).map(f -> this.makeConverter(f.dataType)).toArray(ValueConverter[]::new);
            final String[] fields = Arrays.stream(((StructType) dataType).fields).map(f -> f.name).toArray(String[]::new);
            return obj -> {
                Map<String, Object> map = (Map<String, Object>) obj;
                // nested rows use array node container
                final ArrayNode arrayNode = csvMapper.createArrayNode();
                String field;
                Object value;
                for (int i = 0; i < fields.length; i++) {
                    field = fields[i];
                    value = map.get(field);
                    if (value == null) {
                        arrayNode.add(NullNode.getInstance());
                    }else{
                        arrayNode.add(fieldConverters[i].convert(value));
                    }
                }
                return arrayNode;
            };
        }

        if (dataType instanceof ArrayType) {
            final ValueConverter elementConverter = this.makeConverter(((ArrayType) dataType).elementType);
            return obj -> {
                List<Object> list = (List<Object>) obj;
                Object element;
                ArrayNode arrayNode = csvMapper.createArrayNode();
                for (int i = 0; i < list.size(); i++) {
                    element = list.get(i);
                    if (element == null) {
                        arrayNode.add(NullNode.getInstance());
                    } else {
                        arrayNode.add(elementConverter.convert(element));
                    }
                }
                return arrayNode;
            };
        }

        throw new UnsupportedOperationException("unsupported dataType: " + dataType);
    }

    private JsonNode convertString(Object obj) {
        return TextNode.valueOf(obj.toString());
    }

    private JsonNode convertInteger(Object obj) {
        if(obj instanceof Number){
            return IntNode.valueOf(((Number) obj).intValue());
        } else if(obj instanceof String){
            return IntNode.valueOf(Integer.parseInt((String) obj));
        } else {
            throw new IllegalArgumentException(String.format("can not convert %s to int", obj));
        }
    }

    private JsonNode convertLong(Object obj) {
        if(obj instanceof Number){
            return LongNode.valueOf(((Number) obj).longValue());
        } else if(obj instanceof String){
            return LongNode.valueOf(Long.parseLong((String) obj));
        } else {
            throw new IllegalArgumentException(String.format("can not convert %s to long", obj));
        }
    }

    private JsonNode convertFloat(Object obj) {
        if(obj instanceof Number){
            return FloatNode.valueOf(((Number) obj).floatValue());
        } else if(obj instanceof String){
            return FloatNode.valueOf(Float.parseFloat((String) obj));
        } else {
            throw new IllegalArgumentException(String.format("can not convert %s to float", obj));
        }
    }

    private JsonNode convertDouble(Object obj) {
        if(obj instanceof Number){
            return DoubleNode.valueOf(((Number) obj).doubleValue());
        } else if(obj instanceof String){
            return DoubleNode.valueOf(Double.parseDouble((String) obj));
        } else {
            throw new IllegalArgumentException(String.format("can not convert %s to double", obj));
        }
    }

    private JsonNode convertBoolean(Object obj) {
        if(obj instanceof Boolean){
            return BooleanNode.valueOf((Boolean) obj);
        } else if(obj instanceof String){
            return BooleanNode.valueOf(Boolean.parseBoolean((String) obj));
        } else {
            throw new IllegalArgumentException(String.format("can not convert %s to double", obj));
        }
    }

    @FunctionalInterface
    public interface ValueConverter extends Serializable {
        JsonNode convert(Object obj);
    }

}
