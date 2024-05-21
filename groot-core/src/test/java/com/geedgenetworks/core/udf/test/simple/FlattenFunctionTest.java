package com.geedgenetworks.core.udf.test.simple;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.udf.UDFContext;
import com.geedgenetworks.core.udf.Flatten;
import com.geedgenetworks.core.udf.StringJoiner;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FlattenFunctionTest {

    @BeforeAll
    public static void setUp() {

    }

    // 测试方法

    @Test
    public void testFlattenFunction() {


        UDFContext udfContext = new UDFContext();
        udfContext.setLookup_fields(List.of("parent1", "parent2", "parent3", "parent4"));
        Map<String, Object> params = new HashMap<>();
        params.put("prefix", "prefix");
        params.put("depth", "4");
        params.put("delimiter", "_");
        udfContext.setParameters(params);

        Map<String, Object> nestedMap = Map.of(
                "parent1", List.of(
                        Map.of("firstName", "John", "lastName", "Doe", "age", 23, "children", List.of(
                                Map.of("firstName", "Sally", "lastName", "Green", "age", 1),
                                Map.of("firstName", "Jim", "lastName", "Galley", "age", 2)
                        )),
                        Map.of("firstName", "Mary", "lastName", "Smith", "age", 32)
                ),
                "parent2", List.of(
                        Map.of("firstName", "Sally", "lastName", "Green", "age", 27, "child", Map.of("firstName", "tom", "lastName", "Galley", "age", 41)),
                        Map.of("firstName", "Jim", "lastName", "Galley", "age", 41)
                ),
                "parent3", Map.of("firstName", "Sally", "lastName", "Green", "age", 99),
                "parent4", 55,
                "parent5", Map.of("firstName", "Sally", "lastName", "Green", "age", 99)

        );
        Flatten flatten = new Flatten();
        flatten.open(null, udfContext);
        Event event = new Event();
        event.setExtractedFields(nestedMap);
        Event result = flatten.evaluate(event);
        assertEquals("Jim", result.getExtractedFields().get("prefix_parent1_0_children_1_firstName"));
        assertEquals("tom", result.getExtractedFields().get("prefix_parent2_0_child_firstName"));
        assertEquals("Green", result.getExtractedFields().get("prefix_parent3_lastName"));
        assertEquals("55", result.getExtractedFields().get("prefix_parent4").toString());
        Map<String, Object> map = (Map<String, Object>) result.getExtractedFields().getOrDefault("parent5", new HashMap<>());
        assertEquals("Green", map.get("lastName"));

    }
}
