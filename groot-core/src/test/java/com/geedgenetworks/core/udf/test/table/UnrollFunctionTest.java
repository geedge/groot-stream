package com.geedgenetworks.core.udf.test.table;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.udf.UDFContext;
import com.geedgenetworks.core.udf.udtf.Unroll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class UnrollFunctionTest {

    private static Map<String, Object> nestedMap;
    @BeforeAll
    public static void setUp() {
        nestedMap = Map.of(
                "k1", List.of(
                        Map.of("tunnels_schema_type", "ETHERNET", "source_mac", "52:d4:18:c7:e5:11"),
                        Map.of("tunnels_schema_type", "ETHERNET", "source_mac", "ff:ff:ff:ff:ff:ff")
                ),
                "k2", Map.of("name", "[{\"tunnels_schema_type\":\"MULTIPATH_ETHERNET\",\"source_mac\":\"52:d4:18:c7:e5:11\",\"c2s_destination_mac\":\"10:70:fd:03:c2:6d\",\"s2c_source_mac\":\"10:70:fd:03:c2:6d\",\"s2c_destination_mac\":\"10:70:fd:03:c2:6c\"},{\"tunnels_schema_type\":\"MULTIPATH_ETHERNET\",\"source_mac\":\"ff:ff:ff:ff:ff:ff\",\"c2s_destination_mac\":\"10:70:fd:03:c2:6d\",\"s2c_source_mac\":\"10:70:fd:03:c2:6d\",\"s2c_destination_mac\":\"10:70:fd:03:c2:6c\"}]", "lastName", "{\"tunnels_schema_type\":\"ETHERNET\",\"source_mac\":\"52:d4:18:c7:e5:11\",\"destination_mac\":\"ff:ff:ff:ff:ff:ff\"}"),
                "k3","[{\"tunnels_schema_type\":\"ETHERNET\",\"source_mac\":\"52:d4:18:c7:e5:11\",\"destination_mac\":\"ff:ff:ff:ff:ff:ff\"},{\"tunnels_schema_type\":\"ETHERNET\",\"source_mac\":\"ff:ff:ff:ff:ff:ff\",\"destination_mac\":\"ff:ff:ff:ff:ff:ff\"}]",
                "k4","{\"tunnels_schema_type\":\"ETHERNET\",\"source_mac\":\"52:d4:18:c7:e5:11\",\"destination_mac\":\"ff:ff:ff:ff:ff:ff\"}",
                "k5", Map.of("name",List.of(
                                Map.of("tunnels_schema_type", "ETHERNET", "source_mac", "52:d4:18:c7:e5:11"),
                                Map.of("tunnels_schema_type", "ETHERNET", "source_mac", "ff:ff:ff:ff:ff:ff")
                        )
                ),                "k6", Map.of("name",List.of(
                                Map.of("name2", List.of(
                                        Map.of("tunnels_schema_type", "ETHERNET", "source_mac", "52:d4:18:c7:e5:11"),
                                        Map.of("tunnels_schema_type", "ETHERNET", "source_mac", "ff:ff:ff:ff:ff:ff")
                                ), "source_mac", "52:d4:18:c7:e5:11"),
                                Map.of("tunnels_schema_type", "ETHERNET", "source_mac", "ff:ff:ff:ff:ff:ff")
                        )
                )
        );
    }

    // 测试方法
    @Test
    public void testUnrollFunction1() {


        UDFContext udfContext = new UDFContext();
        udfContext.setLookup_fields(List.of("k1"));
        udfContext.setOutput_fields(List.of("newk1"));
        Map<String, Object> params = new HashMap<>();
        params.put("path", "");
        udfContext.setParameters(params);
        Unroll unroll = new Unroll();
        unroll.open(null, udfContext);
        Event event = new Event();
        event.setExtractedFields(nestedMap);
        List<Event> result = unroll.evaluate(event);
        assertEquals(2, result.size());
        Map<String, Object> map1  = (Map<String, Object>) result.get(0).getExtractedFields().get("newk1");
        assertEquals("52:d4:18:c7:e5:11", map1.get("source_mac"));
        Map<String, Object> map2 = (Map<String, Object>) result.get(1).getExtractedFields().get("newk1");
        assertEquals("ff:ff:ff:ff:ff:ff", map2.get("source_mac"));



    }

    @Test
    public void testUnrollFunction2() {
        UDFContext udfContext = new UDFContext();
        Unroll unroll = new Unroll();
        Event event = new Event();
        event.setExtractedFields(nestedMap);
        udfContext.setLookup_fields(List.of("k3"));
        udfContext.setOutput_fields(List.of("newk3"));
        unroll.open(null, udfContext);
        List<Event> result3 = unroll.evaluate(event);
        assertEquals(2, result3.size());
        Map<String, Object> map3  = (Map<String, Object>) result3.get(0).getExtractedFields().get("newk3");
        assertEquals("52:d4:18:c7:e5:11", map3.get("source_mac"));
        Map<String, Object> map4 = (Map<String, Object>) result3.get(1).getExtractedFields().get("newk3");
        assertEquals("ff:ff:ff:ff:ff:ff", map4.get("source_mac"));


        Map<String, Object> params = new HashMap<>();
        params.put("path", "name");
        udfContext.setParameters(params);
        event.setExtractedFields(nestedMap);
        udfContext.setLookup_fields(List.of("k2"));
        udfContext.setOutput_fields(List.of("newk2"));
        unroll.open(null, udfContext);
        List<Event> result2 = unroll.evaluate(event);
        assertEquals(2, result2.size());
        Map<String, Object> map5  = (Map<String, Object>) result2.get(0).getExtractedFields().get("newk2");
        assertEquals("52:d4:18:c7:e5:11", map5.get("source_mac"));
        Map<String, Object> map6 = (Map<String, Object>) result2.get(1).getExtractedFields().get("newk2");
        assertEquals("ff:ff:ff:ff:ff:ff", map6.get("source_mac"));


        Map<String, Object> params1 = new HashMap<>();
        params.put("path", "name.0.name2");
        udfContext.setParameters(params1);
        event.setExtractedFields(nestedMap);
        udfContext.setLookup_fields(List.of("k6"));
        udfContext.setOutput_fields(List.of("newk6"));
        unroll.open(null, udfContext);
        List<Event> result6 = unroll.evaluate(event);
        assertEquals(2, result6.size());
        Map<String, Object> map9  = (Map<String, Object>) result6.get(0).getExtractedFields().get("newk6");
        assertEquals("52:d4:18:c7:e5:11", map9.get("source_mac"));
        Map<String, Object> map10 = (Map<String, Object>) result6.get(1).getExtractedFields().get("newk6");
        assertEquals("ff:ff:ff:ff:ff:ff", map10.get("source_mac"));
    }
    @Test
    public void testUnrollFunction4() {


        UDFContext udfContext = new UDFContext();
        Unroll unroll = new Unroll();
        Event event = new Event();
        Map<String, Object> params = new HashMap<>();
        udfContext.setParameters(params);
        event.setExtractedFields(nestedMap);
        udfContext.setLookup_fields(List.of("k4"));
        udfContext.setOutput_fields(List.of("newk4"));
        unroll.open(null, udfContext);
        List<Event> result2 = unroll.evaluate(event);
        assertEquals(1, result2.size());

    }
}
