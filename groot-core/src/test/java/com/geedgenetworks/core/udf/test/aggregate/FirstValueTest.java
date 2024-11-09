package com.geedgenetworks.core.udf.test.aggregate;


import com.geedgenetworks.common.config.Accumulator;
import com.geedgenetworks.core.udf.udaf.FirstValue;
import com.geedgenetworks.spi.common.udf.UDFContext;
import com.geedgenetworks.spi.table.event.Event;
import org.junit.jupiter.api.Test;

import java.text.ParseException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FirstValueTest {

    @Test
    public void test() throws ParseException {

        List<String> arr = List.of("192.168.1.1", "192.168.1.2", "192.168.1.3", "192.168.1.4","192.168.1.4");
        List<String> arr2 = List.of("192.168.1.2", "192.168.1.2", "192.168.1.3", "192.168.1.4","192.168.1.4");
        testMerge(arr,arr2);
        testGetResult(arr);

    }
    private void testMerge(List<String> arr,List<String> arr2) {

        UDFContext udfContext = new UDFContext();
        udfContext.setLookupFields(List.of("field"));
        udfContext.setOutputFields(Collections.singletonList("field_first"));
        FirstValue firstValue = new FirstValue();
        Map<String, Object> metricsFields = new HashMap<>();
        Accumulator accumulator = new Accumulator();
        accumulator.setMetricsFields(metricsFields);
        firstValue.open(udfContext);
        Accumulator result1 = getMiddleResult(udfContext,arr);
        Accumulator result2 = getMiddleResult(udfContext,arr2);
        Accumulator result = firstValue.getResult(firstValue.merge(result1,result2));
        String val = (String) result.getMetricsFields().get("field_first");
        assertEquals(val,"192.168.1.1");

    }
    private Accumulator getMiddleResult(UDFContext udfContext,List<String> arr) {


        FirstValue firstValue = new FirstValue();
        Map<String, Object> metricsFields = new HashMap<>();
        Accumulator accumulator = new Accumulator();
        accumulator.setMetricsFields(metricsFields);
        firstValue.open(udfContext);
        Accumulator agg = firstValue.initAccumulator(accumulator);
        for (String o : arr) {
            Event event = new Event();
            Map<String, Object> extractedFields = new HashMap<>();
            extractedFields.put("field", o);
            event.setExtractedFields(extractedFields);
            agg = firstValue.add(event, agg);

        }
        return agg;
    }
    private static void testGetResult(List<String> arr) throws ParseException {

        UDFContext udfContext = new UDFContext();
        udfContext.setLookupFields(List.of("field"));
        udfContext.setOutputFields(Collections.singletonList("field_first"));
        FirstValue firstValue = new FirstValue();
        Map<String, Object> metricsFields = new HashMap<>();
        Accumulator accumulator = new Accumulator();
        accumulator.setMetricsFields(metricsFields);
        firstValue.open(udfContext);
        Accumulator agg = firstValue.initAccumulator(accumulator);
        for (String o : arr) {
            Event event = new Event();
            Map<String, Object> extractedFields = new HashMap<>();
            extractedFields.put("field", o);
            event.setExtractedFields(extractedFields);
            agg = firstValue.add(event, agg);

        }
        Accumulator result = firstValue.getResult(agg);
        String val = (String) result.getMetricsFields().get("field_first");
        assertEquals(val,"192.168.1.1");
    }



}