package com.geedgenetworks.core.udf.test.aggregate;


import com.geedgenetworks.common.Accumulator;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.udf.UDFContext;
import com.geedgenetworks.core.udf.udaf.Min;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MinTest {

    private Min minFunction;
    private UDFContext udfContext;
    private Accumulator acc;
    private Event event1, event2, event3;

    @BeforeEach
    void setUp() {
        minFunction = new Min();
        udfContext = new UDFContext();
        udfContext.setLookupFields(List.of("value"));
        udfContext.setOutputFields(List.of("minValue"));
        minFunction.open(udfContext);

        acc = new Accumulator();
        acc.setMetricsFields(new HashMap<>());

        event1 = new Event();
        event2 = new Event();
        event3 = new Event();
    }

    @Test
    void testAddNumericValue() {
        event1.setExtractedFields(Map.of("value", 5.0));
        event2.setExtractedFields(Map.of("value", 3.0));
        event3.setExtractedFields(Map.of("value", 10.0));

        acc = minFunction.add(event1, acc);
        acc = minFunction.add(event2, acc);
        acc = minFunction.add(event3, acc);

        assertEquals(3.0, acc.getMetricsFields().get("minValue"));
    }

    @Test
    void testAddStringValue() {
        event1.setExtractedFields(Map.of("value", "apple"));
        event2.setExtractedFields(Map.of("value", "banana"));
        event3.setExtractedFields(Map.of("value", "cherry"));

        acc = minFunction.add(event1, acc);
        acc = minFunction.add(event2, acc);
        acc = minFunction.add(event3, acc);

        assertEquals("apple", acc.getMetricsFields().get("minValue"));
    }

    @Test
    void testAddLocalDateTime() {
        event1.setExtractedFields(Map.of("value", LocalDateTime.of(2023, 1, 1, 0, 0)));
        event2.setExtractedFields(Map.of("value", LocalDateTime.of(2022, 1, 1, 0, 0)));
        event3.setExtractedFields(Map.of("value", LocalDateTime.of(2024, 1, 1, 0, 0)));

        acc = minFunction.add(event1, acc);
        acc = minFunction.add(event2, acc);
        acc = minFunction.add(event3, acc);

        assertEquals(LocalDateTime.of(2022, 1, 1, 0, 0), acc.getMetricsFields().get("minValue"));
    }

    @Test
    void testMergeAccumulatorsForNumber() {
        Accumulator acc1 = new Accumulator();
        Accumulator acc2 = new Accumulator();
        Map map1 = new HashMap();
        map1.put("minValue", 10L);
        Map map2 = new HashMap();
        map2.put("minValue", 5.0);
        acc1.setMetricsFields(map1);
        acc2.setMetricsFields(map2);
        Accumulator mergedAcc = minFunction.merge(acc1, acc2);
        assertEquals(5.0, mergedAcc.getMetricsFields().get("minValue"));
    }

    @Test
    void testMergeAccumulatorsForTime() {
        Accumulator acc1 = new Accumulator();
        Accumulator acc2 = new Accumulator();
        Map map1 = new HashMap();
        map1.put("minValue", LocalDateTime.of(2023, 1, 1, 0, 0,1));
        Map map2 = new HashMap();
        map2.put("minValue", LocalDateTime.of(2023, 1, 1, 0, 0,2));
        acc1.setMetricsFields(map1);
        acc2.setMetricsFields(map2);
        Accumulator mergedAcc = minFunction.merge(acc1, acc2);
        assertEquals(LocalDateTime.of(2023, 1, 1, 0, 0,1), mergedAcc.getMetricsFields().get("minValue"));
    }

    @Test
    void testMergeAccumulatorsForString() {
        Accumulator acc1 = new Accumulator();
        Accumulator acc2 = new Accumulator();
        Map map1 = new HashMap();
        map1.put("minValue", "qwe");
        Map map2 = new HashMap();
        map2.put("minValue", "abc");
        acc1.setMetricsFields(map1);
        acc2.setMetricsFields(map2);
        Accumulator mergedAcc = minFunction.merge(acc1, acc2);
        assertEquals("abc", mergedAcc.getMetricsFields().get("minValue"));
    }


}