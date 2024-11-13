package com.geedgenetworks.core.udf.test.aggregate;


import com.geedgenetworks.common.config.Accumulator;
import com.geedgenetworks.core.udf.udaf.LastValue;
import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.connector.event.Event;
import org.junit.jupiter.api.Test;

import java.text.ParseException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LastValueTest {

    @Test
    public void test() throws ParseException {

        List<String> arr = List.of("192.168.1.1", "192.168.1.2", "192.168.1.3", "192.168.1.4","192.168.1.4");
        List<String> arr2 = List.of("192.168.1.2", "192.168.1.2", "192.168.1.3", "192.168.1.4","192.168.1.3");
        testMerge(arr,arr2);
        testGetResult(arr);

    }
    private void testMerge(List<String> arr,List<String> arr2) {

        UDFContext udfContext = new UDFContext();
        udfContext.setLookupFields(List.of("field"));
        udfContext.setOutputFields(Collections.singletonList("field_last"));
        LastValue lastValue = new LastValue();
        Map<String, Object> metricsFields = new HashMap<>();
        Accumulator accumulator = new Accumulator();
        accumulator.setMetricsFields(metricsFields);
        lastValue.open(udfContext);
        Accumulator result1 = getMiddleResult(udfContext,arr);
        Accumulator result2 = getMiddleResult(udfContext,arr2);
        Accumulator result = lastValue.getResult(lastValue.merge(result1,result2));
        String val = (String) result.getMetricsFields().get("field_last");
        assertEquals(val,"192.168.1.3");

    }
    private Accumulator getMiddleResult(UDFContext udfContext,List<String> arr) {


        LastValue lastValue = new LastValue();
        Map<String, Object> metricsFields = new HashMap<>();
        Accumulator accumulator = new Accumulator();
        accumulator.setMetricsFields(metricsFields);
        lastValue.open(udfContext);
        Accumulator agg = lastValue.initAccumulator(accumulator);
        for (String o : arr) {
            Event event = new Event();
            Map<String, Object> extractedFields = new HashMap<>();
            extractedFields.put("field", o);
            event.setExtractedFields(extractedFields);
            agg = lastValue.add(event, agg);

        }
        return agg;
    }
    private static void testGetResult(List<String> arr) throws ParseException {

        UDFContext udfContext = new UDFContext();
        udfContext.setLookupFields(List.of("field"));
        udfContext.setOutputFields(Collections.singletonList("field_last"));
        LastValue lastValue = new LastValue();
        Map<String, Object> metricsFields = new HashMap<>();
        Accumulator accumulator = new Accumulator();
        accumulator.setMetricsFields(metricsFields);
        lastValue.open(udfContext);
        Accumulator agg = lastValue.initAccumulator(accumulator);
        for (String o : arr) {
            Event event = new Event();
            Map<String, Object> extractedFields = new HashMap<>();
            extractedFields.put("field", o);
            event.setExtractedFields(extractedFields);
            agg = lastValue.add(event, agg);

        }
        Accumulator result = lastValue.getResult(agg);
        String val = (String) result.getMetricsFields().get("field_last");
        assertEquals(val,"192.168.1.4");
    }



}