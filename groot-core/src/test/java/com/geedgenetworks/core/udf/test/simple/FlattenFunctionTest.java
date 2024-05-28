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
        udfContext.setLookup_fields(List.of("parent1", "parent2", "parent3", "parent4","parent5","parent6","parent7","parent8"));
        Map<String, Object> params = new HashMap<>();
        params.put("prefix", "prefix");
        params.put("depth", "4");
        params.put("delimiter", "_");
        params.put("json_string_keys", "[parent5_lastName,parent5_firstName,parent6,parent7,parent8]");

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
                "parent5", Map.of("firstName", "[{\"tunnels_schema_type\":\"MULTIPATH_ETHERNET\",\"c2s_source_mac\":\"10:70:fd:03:c2:6c\",\"c2s_destination_mac\":\"10:70:fd:03:c2:6d\",\"s2c_source_mac\":\"10:70:fd:03:c2:6d\",\"s2c_destination_mac\":\"10:70:fd:03:c2:6c\"}]", "lastName", "{\"tunnels_schema_type\":\"ETHERNET\",\"source_mac\":\"52:d4:18:c7:e5:11\",\"destination_mac\":\"ff:ff:ff:ff:ff:ff\"}", "age", 99),
                "parent6","[{\"tunnels_schema_type\":{\"tunnels_schema_type\":[{\"tunnels_schema_type\":{\"tunnels_schema_type\":\"ETHERNET\",\"source_mac\":\"52:d4:18:c7:e5:11\",\"destination_mac\":\"ff:ff:ff:ff:ff:ff\"},\"source_mac\":\"52:d4:18:c7:e5:11\",\"destination_mac\":\"ff:ff:ff:ff:ff:ff\"},{\"tunnels_schema_type\":\"ETHERNET\",\"source_mac\":\"52:d4:18:c7:e5:11\",\"destination_mac\":\"ff:ff:ff:ff:ff:ff\"}],\"source_mac\":\"52:d4:18:c7:e5:11\",\"destination_mac\":\"ff:ff:ff:ff:ff:ff\"},\"source_mac\":\"52:d4:18:c7:e5:11\",\"destination_mac\":\"ff:ff:ff:ff:ff:ff\"},{\"tunnels_schema_type\":\"ETHERNET\",\"source_mac\":\"52:d4:18:c7:e5:11\",\"destination_mac\":\"ff:ff:ff:ff:ff:ff\"}]",
                "parent7","{\"tunnels_schema_type\":\"ETHERNET\",\"source_mac\":\"52:d4:18:c7:e5:11\",\"destination_mac\":\"ff:ff:ff:ff:ff:ff\"}",
                "parent8","error",
                "parent9","other"

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
        assertEquals("ETHERNET", result.getExtractedFields().get("prefix_parent5_lastName_tunnels_schema_type").toString());
        assertEquals("ETHERNET", result.getExtractedFields().get("prefix_parent7_tunnels_schema_type").toString());
        assertEquals("ETHERNET", result.getExtractedFields().get("prefix_parent6_0_tunnels_schema_type_tunnels_schema_type_0_tunnels_schema_type_tunnels_schema_type").toString());
        assertEquals("error", result.getExtractedFields().get("prefix_parent8"));
        assertEquals("other", result.getExtractedFields().get("parent9"));
        assertEquals("10:70:fd:03:c2:6c", result.getExtractedFields().get("prefix_parent5_firstName_0_c2s_source_mac").toString());
    }
}
