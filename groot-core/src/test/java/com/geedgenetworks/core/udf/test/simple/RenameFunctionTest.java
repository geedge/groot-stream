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
        List<String> parent = List.of("parent1_firstName,parent3_firstName.parent3_firstName");
        Map<String, Object> rename_fields = Map.of("parent2_firstName", "parent3_firstName", "parent2_lastName", "parent3_lastName", "parent2_age", "parent3_age", "parent1_firstName", "newkey");
        Map<String, Object> params = new HashMap<>();
        params.put("parent_fields", parent);
        params.put("rename_fields", rename_fields);
        udfContext.setParameters(params);
        Rename rename = new Rename();
        rename.open(null, udfContext);
        Event event = new Event();
        Map<String, Object> extractedFields = Map.of("parent1_firstName", Map.of("parent2_firstName", "parent3_firstName", "parent2_lastName", "parent3_lastName", "parent2_age", "parent3_age"), "parent1_lastName", Map.of("parent2_firstName", "parent3_firstName", "parent2_lastName", "parent3_lastName", "parent2_age", "parent3_age"), "parent1_age", 1, "parent2_firstName", "Jim", "parent2_lastName", "Galley", "parent2_age", 2
                , "parent3_firstName", Map.of("parent3_firstName", Map.of("parent1_firstName", "parent3_firstName.parent3_firstName.parent1_firstName")));
        event.setExtractedFields(extractedFields);
        Event result1 = rename.evaluate(event);
        Map<String, Object> map1 = (Map<String, Object>) result1.getExtractedFields().getOrDefault("parent1_firstName", new HashMap<>());
        assertEquals("parent3_firstName", map1.get("parent3_firstName").toString());
        Map<String, Object> map2 = (Map<String, Object>) result1.getExtractedFields().getOrDefault("parent1_lastName", new HashMap<>());
        assertEquals("parent3_firstName", map2.get("parent2_firstName"));
        Map<String, Object> map3 = (Map<String, Object>) result1.getExtractedFields().getOrDefault("parent3_firstName", new HashMap<>());
        Map<String, Object> map4 = (Map<String, Object>) map3.getOrDefault("parent3_firstName", new HashMap<>());
        assertEquals("parent3_firstName.parent3_firstName.parent1_firstName", map4.get("newkey"));
    }
}
