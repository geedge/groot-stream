package com.geedgenetworks.formats.json;

import com.alibaba.fastjson2.JSON;

import java.util.LinkedHashMap;
import java.util.Map;

public class JsonEventSerializationSchemaTest {


    public static void main(String[] args) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("id", 1);
        map.put("name", "aaa");
        byte[] bytes = JSON.toJSONBytes(map);
        System.out.println(bytes);
    }

}