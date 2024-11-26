package com.geedgenetworks.core.udf.test.aggregate;


import com.geedgenetworks.common.config.Accumulator;
import com.geedgenetworks.core.udf.udaf.CollectList;
import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.event.Event;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class CollectListTest {

    @Test
    public void testObjectType() {
        // 测试默认 "object" 类型
        List<String> arr = List.of("192.168.1.1", "192.168.1.2", "192.168.1.1");
        UDFContext udfContext = createUDFContext();
        CollectList collectList = new CollectList();
        collectList.open(udfContext);

        Accumulator accumulator = initializeAccumulator(collectList, udfContext);
        for (String ip : arr) {
            accumulator = addEvent(collectList, udfContext, accumulator, "field", ip);
        }

        Accumulator result = collectList.getResult(accumulator);
        List<String> aggregated = (List<String>) result.getMetricsFields().get("field_list");
        assertEquals(aggregated.size(), 3);
        assertEquals("192.168.1.1", aggregated.get(0));
    }

    @Test
    public void testArrayType() {
        // 测试 "array" 类型
        List<List<String>> arrays = List.of(
                List.of("192.168.1.1", "192.168.1.2"),
                List.of("192.168.1.1", "192.168.1.2"),
                List.of("192.168.1.3", "192.168.1.4")
        );
        UDFContext udfContext = createUDFContext();
        CollectList collectList = new CollectList();
        collectList.open(udfContext);

        Accumulator accumulator = initializeAccumulator(collectList, udfContext);
        for (List<String> array : arrays) {
            accumulator = addEvent(collectList, udfContext, accumulator, "field", array);
        }

        Accumulator result = collectList.getResult(accumulator);
        List<List<String>> aggregated = (List<List<String>>) result.getMetricsFields().get("field_list");
        assertEquals(aggregated.size(), 3);
    }

    @Test
    public void testMerge() {
        // 测试合并逻辑
        List<String> arr1 = List.of("192.168.1.1", "192.168.1.2");
        List<String> arr2 = List.of("192.168.1.1", "192.168.1.4");

        UDFContext udfContext = createUDFContext();
        CollectList collectList = new CollectList();
        collectList.open(udfContext);

        Accumulator result1 = createAccumulatorFromList(collectList, udfContext, arr1);
        Accumulator result2 = createAccumulatorFromList(collectList, udfContext, arr2);

        Accumulator merged = collectList.merge(result1, result2);
        List<String> aggregated = (List<String>) merged.getMetricsFields().get("field_list");

        assertEquals(aggregated.size(), 4);
    }

    @Test
    public void testEmptyInput() {
        // 测试空输入
        UDFContext udfContext = createUDFContext();
        CollectList collectList = new CollectList();
        collectList.open(udfContext);

        Accumulator accumulator = initializeAccumulator(collectList, udfContext);
        Accumulator result = collectList.getResult(accumulator);

        List<String> aggregated = (List<String>) result.getMetricsFields().get("field_list");
        assertTrue(aggregated.isEmpty());
    }


    private UDFContext createUDFContext() {
        UDFContext udfContext = new UDFContext();
        udfContext.setLookupFields(List.of("field"));
        udfContext.setOutputFields(Collections.singletonList("field_list"));
        Map<String, Object> parameters = new HashMap<>();
        udfContext.setParameters(parameters);
        return udfContext;
    }

    private Accumulator initializeAccumulator(CollectList collectList, UDFContext udfContext) {
        Map<String, Object> metricsFields = new HashMap<>();
        Accumulator accumulator = new Accumulator();
        accumulator.setMetricsFields(metricsFields);
        return collectList.initAccumulator(accumulator);
    }

    private Accumulator createAccumulatorFromList(CollectList collectList, UDFContext udfContext, List<String> values) {
        Accumulator accumulator = initializeAccumulator(collectList, udfContext);
        for (String value : values) {
            accumulator = addEvent(collectList, udfContext, accumulator, "field", value);
        }
        return accumulator;
    }

    private Accumulator addEvent(CollectList collectList, UDFContext udfContext, Accumulator accumulator, String field, Object value) {
        Event event = new Event();
        Map<String, Object> extractedFields = new HashMap<>();
        extractedFields.put(field, value);
        event.setExtractedFields(extractedFields);
        return collectList.add(event, accumulator);
    }
}
