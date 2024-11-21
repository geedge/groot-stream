package com.geedgenetworks.core.udf.test.aggregate;

import com.geedgenetworks.common.config.Accumulator;
import com.geedgenetworks.core.udf.udaf.CollectSet;
import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.event.Event;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class CollectSetTest {

    @Test
    public void testObjectType() {
        // 测试 "object" 类型，验证去重
        List<String> arr = List.of("192.168.1.1", "192.168.1.2", "192.168.1.1");
        UDFContext udfContext = createUDFContext("object");
        CollectSet collectSet = new CollectSet();
        collectSet.open(udfContext);

        Accumulator accumulator = initializeAccumulator(collectSet, udfContext);
        for (String ip : arr) {
            accumulator = addEvent(collectSet, udfContext, accumulator, "field", ip);
        }

        Accumulator result = collectSet.getResult(accumulator);
        Set<String> aggregated = (Set<String>) result.getMetricsFields().get("field_list");
        assertEquals(aggregated.size(), 2);
        assertTrue(aggregated.contains("192.168.1.1"));
        assertTrue(aggregated.contains("192.168.1.2"));
    }

    @Test
    public void testEmptyInput() {
        // 测试空输入
        UDFContext udfContext = createUDFContext("object");
        CollectSet collectSet = new CollectSet();
        collectSet.open(udfContext);

        Accumulator accumulator = initializeAccumulator(collectSet, udfContext);
        Accumulator result = collectSet.getResult(accumulator);

        Set<String> aggregated = (Set<String>) result.getMetricsFields().get("field_list");
        assertNotNull(aggregated);
        assertTrue(aggregated.isEmpty());
    }

    @Test
    public void testMerge() {
        // 测试合并逻辑
        List<String> arr1 = List.of("192.168.1.1", "192.168.1.2");
        List<String> arr2 = List.of("192.168.1.3", "192.168.1.1");

        UDFContext udfContext = createUDFContext("object");
        CollectSet collectSet = new CollectSet();
        collectSet.open(udfContext);

        Accumulator acc1 = createAccumulatorFromList(collectSet, udfContext, arr1);
        Accumulator acc2 = createAccumulatorFromList(collectSet, udfContext, arr2);

        Accumulator merged = collectSet.merge(acc1, acc2);
        Set<String> aggregated = (Set<String>) merged.getMetricsFields().get("field_list");

        assertEquals(aggregated.size(), 3);
        assertTrue(aggregated.containsAll(List.of("192.168.1.1", "192.168.1.2", "192.168.1.3")));
    }

    @Test
    public void testNullValues() {
        // 测试字段为 null 的场景
        UDFContext udfContext = createUDFContext("object");
        CollectSet collectSet = new CollectSet();
        collectSet.open(udfContext);

        Accumulator accumulator = initializeAccumulator(collectSet, udfContext);

        accumulator = addEvent(collectSet, udfContext, accumulator, "field", null);

        Accumulator result = collectSet.getResult(accumulator);
        Set<String> aggregated = (Set<String>) result.getMetricsFields().get("field_list");

        assertNotNull(aggregated);
        assertTrue(aggregated.isEmpty());
    }

    @Test
    public void testDuplicateValues() {
        // 测试重复值是否被正确去重
        List<String> arr = List.of("192.168.1.1", "192.168.1.1", "192.168.1.1");
        UDFContext udfContext = createUDFContext("object");
        CollectSet collectSet = new CollectSet();
        collectSet.open(udfContext);

        Accumulator accumulator = createAccumulatorFromList(collectSet, udfContext, arr);

        Accumulator result = collectSet.getResult(accumulator);
        Set<String> aggregated = (Set<String>) result.getMetricsFields().get("field_list");

        assertEquals(aggregated.size(), 1);
        assertTrue(aggregated.contains("192.168.1.1"));
    }

    private UDFContext createUDFContext(String collectType) {
        UDFContext udfContext = new UDFContext();
        udfContext.setLookupFields(List.of("field"));
        udfContext.setOutputFields(Collections.singletonList("field_list"));
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("collect_type", collectType);
        udfContext.setParameters(parameters);
        return udfContext;
    }

    private Accumulator initializeAccumulator(CollectSet collectSet, UDFContext udfContext) {
        Map<String, Object> metricsFields = new HashMap<>();
        Accumulator accumulator = new Accumulator();
        accumulator.setMetricsFields(metricsFields);
        return collectSet.initAccumulator(accumulator);
    }

    private Accumulator createAccumulatorFromList(CollectSet collectSet, UDFContext udfContext, List<String> values) {
        Accumulator accumulator = initializeAccumulator(collectSet, udfContext);
        for (String value : values) {
            accumulator = addEvent(collectSet, udfContext, accumulator, "field", value);
        }
        return accumulator;
    }

    private Accumulator addEvent(CollectSet collectSet, UDFContext udfContext, Accumulator accumulator, String field, Object value) {
        Event event = new Event();
        Map<String, Object> extractedFields = new HashMap<>();
        extractedFields.put(field, value);
        event.setExtractedFields(extractedFields);
        return collectSet.add(event, accumulator);
    }
}
