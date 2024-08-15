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
                "k2","{\"source_mac\":\"52:d4:18:c7:e5:10\"},{\"source_mac\":\"ff:ff:ff:ff:ff:ff\"},{\"source_mac\":\"52:d4:18:c7:e5:11\"}",
                "k3",""

        );
    }

    // 测试方法
    @Test
    public void testUnrollFunction1() {


        UDFContext udfContext = new UDFContext();
        udfContext.setLookup_fields(List.of("k1"));
        udfContext.setOutput_fields(List.of("newk1"));
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
        Map<String, Object> params = new HashMap<>();
        params.put("regex", ",");
        udfContext.setParameters(params);
        udfContext.setParameters(params);
        event.setExtractedFields(nestedMap);
        udfContext.setLookup_fields(List.of("k2"));
        udfContext.setOutput_fields(List.of("k2"));
        unroll.open(null, udfContext);
        List<Event> result2 = unroll.evaluate(event);
        assertEquals(3, result2.size());
        assertEquals("{\"source_mac\":\"52:d4:18:c7:e5:10\"}", result2.get(0).getExtractedFields().get("k2"));

    }
    @Test
    public void testUnrollFunction3() {


        UDFContext udfContext = new UDFContext();
        Unroll unroll = new Unroll();
        Event event = new Event();
        event.setExtractedFields(nestedMap);
        udfContext.setLookup_fields(List.of("k3"));
        udfContext.setOutput_fields(List.of("newk3"));
        unroll.open(null, udfContext);
        List<Event> result2 = unroll.evaluate(event);
        assertEquals(1, result2.size());

    }
}
