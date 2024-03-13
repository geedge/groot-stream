package com.geedgenetworks.core.connector.schema;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.geedgenetworks.core.types.ArrayType;
import com.geedgenetworks.core.types.DataType;
import com.geedgenetworks.core.types.StructType;
import com.geedgenetworks.core.types.StructType.StructField;
import com.geedgenetworks.core.types.Types;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class SchemaParser {
    public static final String TYPE_BUILTIN = "builtin";
    public static final String TYPE_AVRO = "avro";

    public static final Parser PARSER_BUILTIN = new BuiltinParser();
    public static final Parser PARSER_AVRO = new AvroParser();


    public static Parser getParser(String type){
        if(TYPE_BUILTIN.equals(type)){
            return PARSER_BUILTIN;
        }else if(TYPE_AVRO.equals(type)){
            return PARSER_AVRO;
        }

        throw new UnsupportedOperationException("not supported parser:" + type);
    }

    public static class BuiltinParser implements Parser{
        @Override
        public StructType parser(String content){
            if(JSON.isValidArray(content)){
                return Types.parseSchemaFromJson(content);
            }else{
                return Types.parseStructType(content);
            }
            // throw new IllegalArgumentException("can not parse schema for:" + content);
        }
    }

    public static class AvroParser implements Parser{
        @Override
        public StructType parser(String content) {
            org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(content);
            Set<String> disabledFields = getDisabledFields(JSON.parseObject(content).getJSONArray("fields"));
            return convert2StructType(schema, disabledFields);
        }

        private StructType convert2StructType(org.apache.avro.Schema schema, Set<String> disabledFields){
            List<org.apache.avro.Schema.Field> fields = schema.getFields();
            List<StructField> _fields = new ArrayList<>(fields.size());
            for (int i = 0; i < fields.size(); i++) {
                String fieldName = fields.get(i).name();
                if(disabledFields.contains(fieldName)){
                    continue;
                }
                org.apache.avro.Schema fieldSchema = fields.get(i).schema();
                _fields.add(new StructField(fieldName, convert(fieldSchema)));
            }
            return new StructType(_fields.toArray(new StructField[_fields.size()]));
        }

        private DataType convert(org.apache.avro.Schema schema){
            switch (schema.getType()){
                case INT:
                    return Types.INT;
                case LONG:
                    return Types.BIGINT;
                case FLOAT:
                    return Types.FLOAT;
                case DOUBLE:
                    return Types.DOUBLE;
                case BOOLEAN:
                    return Types.BOOLEAN;
                case STRING:
                    return Types.STRING;
                case BYTES:
                    return Types.BINARY;
                case ARRAY:
                    return new ArrayType(convert(schema.getElementType()));
                case RECORD:
                    return convert2StructType(schema, Collections.EMPTY_SET);
                default:
                    throw new UnsupportedOperationException(schema.toString());
            }
        }

        private Set<String> getDisabledFields(JSONArray fields){
            Set<String> disabledFields = new HashSet<>();
            JSONObject fieldObject;
            JSONObject doc;
            for (int i = 0; i < fields.size(); i++) {
                fieldObject = fields.getJSONObject(i);
                doc = fieldObject.getJSONObject("doc");
                // 过滤禁用的字段
                if(doc != null && "disabled".equals(doc.getString("visibility"))){
                    disabledFields.add(fieldObject.getString("name"));
                }
            }
            return disabledFields;
        }
    }

    public interface Parser extends Serializable {
        StructType parser(String content);
    }
}
