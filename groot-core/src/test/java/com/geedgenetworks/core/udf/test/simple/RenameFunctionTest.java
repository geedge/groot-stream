package com.geedgenetworks.core.udf.test.simple;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.udf.UDFContext;
import com.geedgenetworks.core.udf.Rename;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RenameFunctionTest {

    @BeforeAll
    public static void setUp() {

    }

    // 测试方法

    @Test
    public void testRenameFunction() {

        UDFContext udfContext = new UDFContext();
        Map<String, Object> rename_fields = Map.of("parent2_firstName", "parent3_firstName", "parent2_lastName", "parent3_lastName", "parent2_age", "parent3_age");
        String rename_expression = "key=string.replace_all(key,'parent1_','');key=string.replace_all(key,'parent3_','parent4_');return key;";
        Map<String, Object> params = new HashMap<>();
        params.put("rename_fields", rename_fields);
        params.put("rename_expression", rename_expression);
        udfContext.setParameters(params);
        Rename rename = new Rename();
        rename.open(null, udfContext);
        Event event = new Event();
        Map<String, Object> extractedFields = Map.of("parent1_firstName", "Sally", "parent1_lastName", "Green", "parent1_age", 1, "parent2_firstName", "Jim", "parent2_lastName", "Galley", "parent2_age", 2
        );
        event.setExtractedFields(extractedFields);
        Event result1 = rename.evaluate(event);
        assertEquals("2", result1.getExtractedFields().get("parent4_age").toString());
        assertEquals("Sally", result1.getExtractedFields().get("firstName"));

    }


    @Test
    public void testRenameFunctionWithouExpression() {

        UDFContext udfContext = new UDFContext();
        Map<String, Object> rename_fields = Map.of("k1", "k1_rename","k1_1","k1_1_rename","k3_1_1","k3_1_1_rename","k3_1","k3_1_rename","k3_1_1_1","k3_1_1_1_rename","k3_1_2","k3_1_2_rename");
        Map<String, Object> params = new HashMap<>();
        params.put("parent_fields", "[\"k1\",\"k3\",\"k3.k3_1.k3_1_1\",\"k3.k3_1\"]");
        params.put("rename_fields", rename_fields);
        udfContext.setParameters(params);
        Rename rename = new Rename();
        rename.open(null, udfContext);
        Event event = new Event();
        Map<String, Object> extractedFields = Map.of("k1", Map.of("k1_1", "k1_1_v", "k1_2", "k1_2_v"),"k2","k2_v","k3", Map.of("k3_1", Map.of("k3_1_1", Map.of("k3_1_1_1", "k3_1_1_1_v", "k3_1_1_2", "k3_1_1_2_v"), "k3_1_2", "k3_1_2_v"), "k3_2", "k3_2_v"));
        event.setExtractedFields(extractedFields);
        Event result1 = rename.evaluate(event);
        Map<String, Object> map1 = (Map<String, Object>) result1.getExtractedFields().getOrDefault("k1_rename", new HashMap<>());
        assertEquals("k1_1_v", map1.get("k1_1_rename").toString());
        Map<String, Object> map2 = (Map<String, Object>) result1.getExtractedFields().getOrDefault("k3", new HashMap<>());
        assertEquals("k3_2_v", map2.get("k3_2"));
        Map<String, Object> map3 = (Map<String, Object>) result1.getExtractedFields().getOrDefault("k3", new HashMap<>());
        Map<String, Object> map4 = (Map<String, Object>) map3.getOrDefault("k3_1_rename", new HashMap<>());
        assertEquals("k3_1_2_v", map4.get("k3_1_2_rename"));
        Map<String, Object> map5 = (Map<String, Object>) map3.getOrDefault("k3_1_rename", new HashMap<>());
        Map<String, Object> map6 = (Map<String, Object>) map5.getOrDefault("k3_1_1_rename", new HashMap<>());
        assertEquals("k3_1_1_1_v", map6.get("k3_1_1_1_rename"));

    }
}
