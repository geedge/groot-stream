package com.geedgenetworks.core.udf.test.aggregate;


import com.geedgenetworks.common.config.Accumulator;
import com.geedgenetworks.core.udf.udaf.LongCount;
import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.connector.event.Event;
import org.junit.jupiter.api.Test;

import java.text.ParseException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LongCountTest {

    @Test
    public void test() throws ParseException {

        Long[] longArr = new Long[]{1L, 2L, 3L, 4L};
        Long[] longArr2 = new Long[]{1L, 2L, 3L, 4L};
        testMerge(longArr,longArr2);
        testGetResult(longArr);
    }
    private void testMerge(Number[] arr,Number[] arr2) {

        UDFContext udfContext = new UDFContext();
        udfContext.setLookupFields(List.of("field"));
        udfContext.setOutputFields(Collections.singletonList("count"));
        LongCount longCount = new LongCount();
        Map<String, Object> metricsFields = new HashMap<>();
        Accumulator accumulator = new Accumulator();
        accumulator.setMetricsFields(metricsFields);
        longCount.open(udfContext);
        Accumulator result1 = getMiddleResult(udfContext,arr);
        Accumulator result2 = getMiddleResult(udfContext,arr2);
        Accumulator result = longCount.getResult(longCount.merge(result1,result2));
        assertEquals(Integer.parseInt((result.getMetricsFields().get("count").toString())),8);

    }
    private Accumulator getMiddleResult(UDFContext udfContext,Number[] arr) {


        LongCount longCount = new LongCount();
        Map<String, Object> metricsFields = new HashMap<>();
        Accumulator accumulator = new Accumulator();
        accumulator.setMetricsFields(metricsFields);
        longCount.open(udfContext);
        Accumulator agg = longCount.initAccumulator(accumulator);
        for (Number o : arr) {
            Event event = new Event();
            Map<String, Object> extractedFields = new HashMap<>();
            extractedFields.put("field", o);
            event.setExtractedFields(extractedFields);
            agg = longCount.add(event, agg);

        }
        return agg;
    }
    private static void testGetResult(Number[] arr) throws ParseException {

        UDFContext udfContext = new UDFContext();
        udfContext.setOutputFields(Collections.singletonList("count"));
        LongCount longCount = new LongCount();
        Map<String, Object> metricsFields = new HashMap<>();
        Accumulator accumulator = new Accumulator();
        accumulator.setMetricsFields(metricsFields);
        longCount.open(udfContext);
        Accumulator agg = longCount.initAccumulator(accumulator);
        for (Number o : arr) {
            Event event = new Event();
            Map<String, Object> extractedFields = new HashMap<>();
            extractedFields.put("field", o);
            event.setExtractedFields(extractedFields);
            agg = longCount.add(event, agg);

        }
        Accumulator result = longCount.getResult(agg);
        assertEquals(Integer.parseInt((result.getMetricsFields().get("count").toString())),4);
    }




}