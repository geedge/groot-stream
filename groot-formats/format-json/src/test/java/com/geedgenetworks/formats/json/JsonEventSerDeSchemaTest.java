package com.geedgenetworks.formats.json;

import com.alibaba.fastjson2.JSON;
import com.geedgenetworks.api.connector.type.StructType;
import com.geedgenetworks.api.connector.type.Types;
import com.geedgenetworks.api.event.Event;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class JsonEventSerDeSchemaTest {

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

        JsonEventDeserializationSchema deser = new JsonEventDeserializationSchema(null, true);
        JsonEventSerializationSchema ser = new JsonEventSerializationSchema(null);
        deser.open(null);
        ser.open(null);

        byte[] bytes = ser.serialize(row);
        Event rst = deser.deserialize(bytes);
        Map<String, Object> rstMap = rst.getExtractedFields();
        System.out.println(new String(bytes, StandardCharsets.UTF_8));
        System.out.println(JSON.toJSONString(map));
        System.out.println(JSON.toJSONString(rstMap));
        assertTrue(objEquals(map, rstMap, true), "\n" + JSON.toJSONString(map) + "\n" + JSON.toJSONString(rstMap));

        StructType dataType = Types.parseStructType("int:int,bigint:bigint,double:double,string:string,int_array:array<int>,struct:struct<int:int,string:string>");
        deser = new JsonEventDeserializationSchema(dataType, true);
        ser = new JsonEventSerializationSchema(dataType);
        deser.open(null);
        ser.open(null);

        bytes = ser.serialize(row);
        rst = deser.deserialize(bytes);
        rstMap = rst.getExtractedFields();
        System.out.println(new String(bytes, StandardCharsets.UTF_8));
        System.out.println(JSON.toJSONString(map));
        System.out.println(JSON.toJSONString(rstMap));
        assertTrue(objEquals(map, rstMap, true),  "\n" + JSON.toJSONString(map) + "\n" + JSON.toJSONString(rstMap));
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
