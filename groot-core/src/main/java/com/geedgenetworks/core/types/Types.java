package com.geedgenetworks.core.types;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.geedgenetworks.core.types.StructType.StructField;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Types {
    public static final IntegerType INT = new IntegerType();
    public static final LongType BIGINT = new LongType();
    public static final StringType STRING = new StringType();
    public static final FloatType FLOAT = new FloatType();
    public static final DoubleType DOUBLE = new DoubleType();

    public static StructType parseSchemaFromJson(String jsonFields) {
        JSONArray fieldArray = JSON.parseArray(jsonFields);
        StructField[] fields = new StructField[fieldArray.size()];

        for (int i = 0; i < fieldArray.size(); i++) {
            JSONObject fieldObject = fieldArray.getJSONObject(i);
            String name = fieldObject.getString("name");
            String type = fieldObject.getString("type").toLowerCase().trim();
            DataType dataType = parseDataType(type);
            fields[i] = new StructField(name, dataType);
        }

        return new StructType(fields);
    }

    private static DataType parseDataType(String type){

        if("int".equals(type)){
           return INT;
        } else if ("bigint".equals(type)){
            return BIGINT;
        } else if ("string".equals(type)){
            return STRING;
        } else if ("float".equals(type)){
            return FLOAT;
        } else if ("double".equals(type)){
            return DOUBLE;
        }

        // array类型
        Pattern arrayRe = Pattern.compile("array<(.+)>", Pattern.CASE_INSENSITIVE );
        Matcher matcher = arrayRe.matcher(type);
        if(matcher.matches()){
            String eleType = matcher.group(1);
            DataType elementType = parseDataType(eleType);
            return new ArrayType(elementType);
        }

        // struct类型
        // TODO

        throw new UnsupportedOperationException("不支持的类型:" + type);
    }
}
