package com.geedgenetworks.core.udf.test.aggregate;
import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.event.Event;
import com.geedgenetworks.common.config.Accumulator;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.core.udf.udaf.array.ArrayContactAgg;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class ArrayContactAggTest {

    private ArrayContactAgg aggFunction;
    private UDFContext udfContext;
    private Accumulator accumulator;

    @BeforeEach
    void setUp() {
        aggFunction = new ArrayContactAgg();
        udfContext = new UDFContext();
        udfContext.setLookupFields(Collections.singletonList("field1"));
        udfContext.setParameters(new HashMap<>());
        udfContext.setOutputFields(Collections.singletonList("outputField"));
        udfContext.getParameters().put("max_size", 10L);
        accumulator = new Accumulator();
        accumulator.setMetricsFields(new HashMap<>());
    }

    @Test
    void testOpenWithValidParameters() {
        udfContext.getParameters().put("mode", "all");
        udfContext.getParameters().put("max_size", 10L);

        assertDoesNotThrow(() -> aggFunction.open(udfContext));
    }

    @Test
    void testOpenWithInvalidParameters() {
        UDFContext invalidContext = new UDFContext();
        assertThrows(GrootStreamRuntimeException.class, () -> aggFunction.open(invalidContext));

        invalidContext.setLookupFields(Collections.emptyList());
        assertThrows(GrootStreamRuntimeException.class, () -> aggFunction.open(invalidContext));
    }

    @Test
    void testInitAccumulatorWithNullOutputField() {
        udfContext.setOutputFields(null);
        udfContext.getParameters().put("mode", "all");
        aggFunction.open(udfContext);

        Accumulator acc = aggFunction.initAccumulator(accumulator);
        assertTrue(acc.getMetricsFields().containsKey("field1"));
    }

    @Test
    void testAddWithNullValue() {
        udfContext.getParameters().put("mode", "all");
        aggFunction.open(udfContext);
        aggFunction.initAccumulator(accumulator);

        Event event = new Event();

        Map map = new HashMap();
        map.put("field1", null);
        event.setExtractedFields(map);

        aggFunction.add(event, accumulator);
        List<?> list = (List<?>) accumulator.getMetricsFields().get("outputField");
        assertTrue(list.isEmpty());
    }

    @Test
    void testAddWithMultipleValuesAndDuplicates() {
        udfContext.getParameters().put("mode", "all");
        aggFunction.open(udfContext);
        aggFunction.initAccumulator(accumulator);

        Event event1 = new Event();
        event1.setExtractedFields(Map.of("field1", List.of("value1", "value2", "value2")));
        aggFunction.add(event1, accumulator);

        Event event2 = new Event();
        event2.setExtractedFields(Map.of("field1", List.of("value3", "value2")));
        aggFunction.add(event2, accumulator);

        List<?> list = (List<?>) accumulator.getMetricsFields().get("outputField");

        assertEquals(5, list.size());
        assertEquals(Arrays.asList("value1", "value2", "value2", "value3", "value2"), list);
    }

    @Test
    void testAddWithDistinctModeAndDuplicates() {
        udfContext.getParameters().put("mode", "distinct");
        aggFunction.open(udfContext);
        aggFunction.initAccumulator(accumulator);

        Event event1 = new Event();
        event1.setExtractedFields(Map.of("field1", List.of("value1", "value2", "value2")));
        aggFunction.add(event1, accumulator);

        Event event2 = new Event();
        event2.setExtractedFields(Map.of("field1", List.of("value3", "value2")));
        aggFunction.add(event2, accumulator);

        Set<?> set = (Set<?>) accumulator.getMetricsFields().get("outputField");

        assertEquals(3, set.size());
        assertTrue(set.containsAll(Set.of("value1", "value2", "value3")));
    }

    @Test
    void testMergeAccumulatorsWithAllMode() {
        udfContext.getParameters().put("mode", "all");
        aggFunction.open(udfContext);

        Accumulator acc1 = aggFunction.initAccumulator(accumulator);
        Accumulator acc2 = new Accumulator();
        acc2.setMetricsFields(new HashMap<>());
        aggFunction.initAccumulator(acc2);

        List<String> list1 = (List<String>) acc1.getMetricsFields().get("outputField");
        List<String> list2 = (List<String>)acc2.getMetricsFields().get("outputField");

        list1.addAll(Arrays.asList("value1", "value2"));
        list2.addAll(Arrays.asList("value3", "value4"));

        aggFunction.merge(acc1, acc2);

        assertEquals(Arrays.asList("value1", "value2", "value3", "value4"), list1);
    }

    @Test
    void testMergeAccumulatorsWithDistinctMode() {
        udfContext.getParameters().put("mode", "distinct");
        udfContext.getParameters().put("max_size", 10L);

        aggFunction.open(udfContext);

        Accumulator acc1 = aggFunction.initAccumulator(accumulator);
        Accumulator acc2 = new Accumulator();
        acc2.setMetricsFields(new HashMap<>());
        aggFunction.initAccumulator(acc2);

        Set<String> set1 = (Set<String>) acc1.getMetricsFields().get("outputField");
        Set<String> set2 = (Set<String>) acc2.getMetricsFields().get("outputField");

        set1.addAll(Set.of("value1", "value2"));
        set2.addAll(Set.of("value2", "value3"));

        aggFunction.merge(acc1, acc2);

        assertEquals(3, set1.size());
        assertTrue(set1.containsAll(Set.of("value1", "value2", "value3")));
    }

    @Test
    void testAddWithMaxSizeExceeded() {
        udfContext.getParameters().put("mode", "all");
        udfContext.getParameters().put("max_size", 3L);
        aggFunction.open(udfContext);
        aggFunction.initAccumulator(accumulator);

        Event event1 = new Event();
        event1.setExtractedFields(Map.of("field1", List.of("value1", "value2", "value3", "value4")));
        aggFunction.add(event1, accumulator);
        aggFunction.getResult(accumulator);
        List<?> list = (List<?>) accumulator.getMetricsFields().get("outputField");

        assertEquals(3, list.size());
        assertEquals(Arrays.asList("value1", "value2", "value3"), list); // 超出限制后拒绝添加
    }

    @Test
    void testAddWithEmptyInputField() {
        udfContext.getParameters().put("mode", "all");
        aggFunction.open(udfContext);
        aggFunction.initAccumulator(accumulator);

        Event event = new Event();
        event.setExtractedFields(Map.of()); // Missing "field1"

        aggFunction.add(event, accumulator);
        List<?> list = (List<?>) accumulator.getMetricsFields().get("outputField");

        assertTrue(list.isEmpty());
    }
}


