package com.geedgenetworks.core.udf.test.table;

import com.geedgenetworks.core.udf.udtf.JsonUnroll;
import com.geedgenetworks.api.common.udf.UDFContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import com.geedgenetworks.api.connector.event.Event;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class JsonUnrollFunctionTest {

    private static Map<String, Object> nestedMap;
    @BeforeAll
    public static void setUp() {
        nestedMap = Map.of(
                "k1","[{\"tunnels_schema_type\":\"ETHERNET\",\"source_mac\":\"52:d4:18:c7:e5:11\",\"destination_mac\":\"ff:ff:ff:ff:ff:ff\"},{\"tunnels_schema_type\":\"ETHERNET\",\"source_mac\":\"ff:ff:ff:ff:ff:ff\",\"destination_mac\":\"ff:ff:ff:ff:ff:ff\"}]",
                "k2","{\"k2_1\":\"[{\"tunnels_schema_type\":\"ETHERNET\",\"source_mac\":\"52:d4:18:c7:e5:11\",\"destination_mac\":\"ff:ff:ff:ff:ff:ff\"},{\"tunnels_schema_type\":\"ETHERNET\",\"source_mac\":\"ff:ff:ff:ff:ff:ff\",\"destination_mac\":\"ff:ff:ff:ff:ff:ff\"}]\",\"source_mac\":\"52:d4:18:c7:e5:11\",\"destination_mac\":\"ff:ff:ff:ff:ff:ff\"}",
                "k3","{\n" +
                        "    \"k3_1\": {\n" +
                        "        \"k3_1_1\": [\n" +
                        "            {\n" +
                        "                \"tunnels_schema_type\": \"ETHERNET\",\n" +
                        "                \"source_mac\": \"52:d4:18:c7:e5:11\"\n" +
                        "            },\n" +
                        "            {\n" +
                        "                \"tunnels_schema_type\": \"ETHERNET\",\n" +
                        "                \"source_mac\": \"ff:ff:ff:ff:ff:ff\"\n" +
                        "            }\n" +
                        "        ],\n" +
                        "        \"k3_1_2\": {\n" +
                        "            \"tunnels_schema_type\": \"ETHERNET\",\n" +
                        "            \"source_mac\": 19.95\n" +
                        "        }\n" +
                        "    }\n" +
                        "}",
                "k4",""

                );
    }

    // 测试方法

    @Test
    public void testJsonUnrollFunction1() {
        UDFContext udfContext = new UDFContext();
        JsonUnroll unroll = new JsonUnroll();
        Event event = new Event();
        event.setExtractedFields(nestedMap);
        udfContext.setLookupFields(List.of("k1"));
        udfContext.setOutputFields(List.of("newk1"));
        unroll.open(null, udfContext);
        List<Event> result3 = unroll.evaluate(event);
        assertEquals(2, result3.size());
        assertEquals("{\"destination_mac\":\"ff:ff:ff:ff:ff:ff\",\"source_mac\":\"52:d4:18:c7:e5:11\",\"tunnels_schema_type\":\"ETHERNET\"}", result3.get(0).getExtractedFields().get("newk1").toString());
        assertEquals("{\"destination_mac\":\"ff:ff:ff:ff:ff:ff\",\"source_mac\":\"ff:ff:ff:ff:ff:ff\",\"tunnels_schema_type\":\"ETHERNET\"}", result3.get(1).getExtractedFields().get("newk1").toString());




    }
    @Test
    public void testJsonUnrollFunction2() {


        UDFContext udfContext = new UDFContext();
        JsonUnroll unroll = new JsonUnroll();
        Event event = new Event();
        Map<String, Object> params = new HashMap<>();
        udfContext.setParameters(params);
        params.put("path", "$.k3_1.k3_1_1");
        event.setExtractedFields(nestedMap);
        udfContext.setLookupFields(List.of("k3"));
        udfContext.setOutputFields(List.of("newk3"));
        unroll.open(null, udfContext);
        List<Event> result2 = unroll.evaluate(event);
        assertEquals(2, result2.size());
        assertEquals("{\"k3_1\":{\"k3_1_2\":{\"tunnels_schema_type\":\"ETHERNET\",\"source_mac\":19.95},\"k3_1_1\":{\"tunnels_schema_type\":\"ETHERNET\",\"source_mac\":\"52:d4:18:c7:e5:11\"}}}",result2.get(0).getExtractedFields().get("newk3").toString());
        assertEquals("{\"k3_1\":{\"k3_1_2\":{\"tunnels_schema_type\":\"ETHERNET\",\"source_mac\":19.95},\"k3_1_1\":{\"tunnels_schema_type\":\"ETHERNET\",\"source_mac\":\"ff:ff:ff:ff:ff:ff\"}}}",result2.get(1).getExtractedFields().get("newk3").toString());

    }
    @Test
    public void testJsonUnrollFunction3() {


        UDFContext udfContext = new UDFContext();
        JsonUnroll unroll = new JsonUnroll();
        Event event = new Event();
        Map<String, Object> params = new HashMap<>();
        udfContext.setParameters(params);
        params.put("path", "$.k4_1.k4_1_1");
        event.setExtractedFields(nestedMap);
        udfContext.setLookupFields(List.of("k4"));
        udfContext.setOutputFields(List.of("newk4"));
        unroll.open(null, udfContext);
        List<Event> result2 = unroll.evaluate(event);
        assertEquals(1, result2.size());

    }
}
