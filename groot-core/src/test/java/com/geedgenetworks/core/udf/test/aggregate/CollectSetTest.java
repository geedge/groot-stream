package com.geedgenetworks.core.udf.test.aggregate;


import com.geedgenetworks.common.config.Accumulator;
import com.geedgenetworks.core.udf.udaf.CollectSet;
import com.geedgenetworks.spi.common.udf.UDFContext;
import com.geedgenetworks.spi.table.event.Event;
import org.junit.jupiter.api.Test;

import java.text.ParseException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CollectSetTest {

    @Test
    public void test() throws ParseException {

        List<String> arr = List.of("192.168.1.1", "192.168.1.2", "192.168.1.3", "192.168.1.4","192.168.1.4");
        List<String> arr2 = List.of("192.168.1.5", "192.168.1.6", "192.168.1.3", "192.168.1.4");
        testMerge(arr,arr2);
        testGetResult(arr);

    }

    private void testMerge(List<String> arr,List<String> arr2) {

        UDFContext udfContext = new UDFContext();
        udfContext.setLookupFields(List.of("field"));
        udfContext.setOutputFields(Collections.singletonList("field_list"));
        CollectSet collectSet = new CollectSet();
        Map<String, Object> metricsFields = new HashMap<>();
        Accumulator accumulator = new Accumulator();
        accumulator.setMetricsFields(metricsFields);
        collectSet.open(udfContext);
        Accumulator result1 = getMiddleResult(udfContext,arr);
        Accumulator result2 = getMiddleResult(udfContext,arr2);
        Accumulator result = collectSet.getResult(collectSet.merge(result1,result2));
        Set<String> vals = (Set<String>) result.getMetricsFields().get("field_list");
        assertEquals(vals.size(),6);

    }
    private Accumulator getMiddleResult(UDFContext udfContext,List<String> arr) {


        CollectSet collectSet = new CollectSet();
        Map<String, Object> metricsFields = new HashMap<>();
        Accumulator accumulator = new Accumulator();
        accumulator.setMetricsFields(metricsFields);
        collectSet.open(udfContext);
        Accumulator agg = collectSet.initAccumulator(accumulator);

        for (String o : arr) {
            Event event = new Event();
            Map<String, Object> extractedFields = new HashMap<>();
            extractedFields.put("field", o);
            event.setExtractedFields(extractedFields);
            agg = collectSet.add(event, agg);

        }
        return agg;
    }
    private static void testGetResult(List<String> arr) throws ParseException {

        UDFContext udfContext = new UDFContext();
        udfContext.setLookupFields(List.of("field"));
        udfContext.setOutputFields(Collections.singletonList("field_list"));
        CollectSet collectSet = new CollectSet();
        Map<String, Object> metricsFields = new HashMap<>();
        Accumulator accumulator = new Accumulator();
        accumulator.setMetricsFields(metricsFields);
        collectSet.open(udfContext);
        Accumulator agg = collectSet.initAccumulator(accumulator);
        for (String o : arr) {
            Event event = new Event();
            Map<String, Object> extractedFields = new HashMap<>();
            extractedFields.put("field", o);
            event.setExtractedFields(extractedFields);
            agg = collectSet.add(event, agg);

        }
        Accumulator result = collectSet.getResult(agg);
        Set<String> vals = (Set<String>) result.getMetricsFields().get("field_list");
        assertEquals(vals.size(),4);
    }



}