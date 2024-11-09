package com.geedgenetworks.formats.json;

import com.alibaba.fastjson2.JSON;
import com.geedgenetworks.spi.table.type.StructType;
import com.geedgenetworks.spi.table.type.Types;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class JsonSerializerTest {

    @Test
    public void testSerSimpleData(){
        ThreadLocalRandom random = ThreadLocalRandom.current();
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("int", random.nextInt(1, Integer.MAX_VALUE));
        map.put("int_null", null);
        map.put("int_str", Integer.toString(random.nextInt(1, Integer.MAX_VALUE)));

        map.put("int64", random.nextLong(1, Long.MAX_VALUE));
        map.put("int64_null", null);
        map.put("int64_str", Long.toString(random.nextLong(1, Long.MAX_VALUE)));

        map.put("double", random.nextDouble(1, Integer.MAX_VALUE));
        map.put("double_null", null);
        map.put("double_str", Double.toString(random.nextDouble(1, Integer.MAX_VALUE)));

        map.put("str", "ut8字符串");
        map.put("str_null", null);
        map.put("str_int", random.nextInt(1, Integer.MAX_VALUE));

        map.put("int32_array", Arrays.asList(1, 3, 5));
        map.put("int32_array_null", null);
        map.put("int32_array_empty", Collections.emptyList());

        map.put("int64_array", Arrays.asList(1, 3, 5));
        map.put("int64_array_null", null);
        map.put("int64_array_empty", Collections.emptyList());

        map.put("str_array", Arrays.asList(1, 3, 5));

        Map<String, Object> obj = new LinkedHashMap<>();
        obj.put("id", 1);
        obj.put("name", "name");
        map.put("obj", obj);

        List<Object> list = new ArrayList<>();
        list.add(obj);
        obj = new LinkedHashMap<>();
        obj.put("id", 2);
        obj.put("name", "name2");
        list.add(obj);
        map.put("obj_array", list);

        StructType dataType = Types.parseStructType("int: int, int_null:  int, int_str: int, int64:  bigint, int64_null:  bigint, int64_str:  bigint, double:  double, double_null:  double, double_str:  double, " +
                "str:  string, str_null:  string, str_int:  string, int32_array:  array<int>, int32_array_null:  array<int>, int32_array_empty:  array<int>, int64_array:  array<bigint>, int64_array_null:  array<bigint>, int64_array_empty:  array<bigint>," +
                " str_array : array<string>, obj : struct<id :int, name: string>, obj_array : array<struct<id :int, name: string>>");
        JsonSerializer serializer = new JsonSerializer(dataType);

        byte[] bytes = serializer.serialize(map);
        System.out.println(map);
        System.out.println(bytes.length);
        System.out.println(new String(bytes, StandardCharsets.UTF_8));
        System.out.println(JSON.toJSONString(map));

        JsonToMapDataConverter converter = new JsonToMapDataConverter(dataType, false);
        Map<String, Object> rst = converter.convert(new String(bytes, StandardCharsets.UTF_8));
        System.out.println(map);
        System.out.println(rst);

        System.out.println(serializer.serialize(rst).length);
        System.out.println(new String(serializer.serialize(rst), StandardCharsets.UTF_8));
        System.out.println(JSON.toJSONString(map));
    }


}