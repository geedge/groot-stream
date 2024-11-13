package com.geedgenetworks.core.udf.test.aggregate;


import com.geedgenetworks.common.config.Accumulator;
import com.geedgenetworks.core.udf.udaf.Max;
import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.event.Event;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class MaxTest {

    private Max maxFunction;
    private Accumulator acc;
    private Event event;

    @BeforeEach
    void setUp() {
        maxFunction = new Max();
        acc = new Accumulator();
        event = new Event();

        // 初始化上下文
        UDFContext udfContext = new UDFContext();
        udfContext.setLookupFields(List.of("value"));
        udfContext.setOutputFields(List.of("maxValue"));
        maxFunction.open(udfContext);

        // 初始化累加器的 metricsFields
        acc.setMetricsFields(new HashMap<>());
    }

    @Test
    void testAddMultipleNumericValues() {
        // 设置事件中的多个数值
        Map<String, Object> extractedFields = new HashMap<>();
        extractedFields.put("value", 100);
        event.setExtractedFields(extractedFields);
        Accumulator result = maxFunction.add(event, acc);

        // 添加第二个数值
        extractedFields.put("value", 200);
        result = maxFunction.add(event, result);

        // 添加第三个数值
        extractedFields.put("value", 150);
        result = maxFunction.add(event, result);

        // 验证最大值应该是200
        assertEquals(200, result.getMetricsFields().get("maxValue"));
    }

    @Test
    void testAddMultipleStringValues() {
        // 设置事件中的多个字符串值
        Map<String, Object> extractedFields = new HashMap<>();
        extractedFields.put("value", "abc");
        event.setExtractedFields(extractedFields);
        Accumulator result = maxFunction.add(event, acc);

        // 添加第二个字符串值
        extractedFields.put("value", "def");
        result = maxFunction.add(event, result);

        // 添加第三个字符串值
        extractedFields.put("value", "ghi");
        result = maxFunction.add(event, result);

        // 验证最大字符串值（按字母顺序最大的是 "ghi"）
        assertEquals("ghi", result.getMetricsFields().get("maxValue"));
    }

    @Test
    void testAddMultipleLocalDateTimes() {
        // 设置事件中的多个时间值
        Map<String, Object> extractedFields = new HashMap<>();

        extractedFields.put("value", LocalDateTime.of(2024, 10, 22, 12, 0));
        event.setExtractedFields(extractedFields);
        Accumulator result = maxFunction.add(event, acc);

        // 添加第二个时间值
        extractedFields.put("value", LocalDateTime.of(2024, 10, 23, 12, 0));
        result = maxFunction.add(event, result);

        // 添加第三个时间值
        extractedFields.put("value", LocalDateTime.of(2024, 10, 21, 12, 0));
        result = maxFunction.add(event, result);

        // 验证最大时间值（最大的是2024年10月23日12点）
        assertEquals(LocalDateTime.of(2024, 10, 23, 12, 0), result.getMetricsFields().get("maxValue"));
    }

    @Test
    void testMergeAccumulatorsForNumber() {
        Accumulator acc1 = new Accumulator();
        Accumulator acc2 = new Accumulator();
        Map map1 = new HashMap();
        map1.put("maxValue", 10L);
        Map map2 = new HashMap();
        map2.put("maxValue", 5.0);
        acc1.setMetricsFields(map1);
        acc2.setMetricsFields(map2);
        Accumulator mergedAcc = maxFunction.merge(acc1, acc2);
        assertEquals(10L, mergedAcc.getMetricsFields().get("maxValue"));
    }

    @Test
    void testMergeAccumulatorsForTime() {
        Accumulator acc1 = new Accumulator();
        Accumulator acc2 = new Accumulator();
        Map map1 = new HashMap();
        map1.put("maxValue", LocalDateTime.of(2023, 1, 1, 0, 0,1));
        Map map2 = new HashMap();
        map2.put("maxValue", LocalDateTime.of(2023, 1, 1, 0, 0,2));
        acc1.setMetricsFields(map1);
        acc2.setMetricsFields(map2);
        Accumulator mergedAcc = maxFunction.merge(acc1, acc2);
        assertEquals(LocalDateTime.of(2023, 1, 1, 0, 0,2), mergedAcc.getMetricsFields().get("maxValue"));
    }

    @Test
    void testMergeAccumulatorsForString() {
        Accumulator acc1 = new Accumulator();
        Accumulator acc2 = new Accumulator();
        Map map1 = new HashMap();
        map1.put("maxValue", "qwe");
        Map map2 = new HashMap();
        map2.put("maxValue", "abc");
        acc1.setMetricsFields(map1);
        acc2.setMetricsFields(map2);
        Accumulator mergedAcc = maxFunction.merge(acc1, acc2);
        assertEquals("qwe", mergedAcc.getMetricsFields().get("maxValue"));
    }
}