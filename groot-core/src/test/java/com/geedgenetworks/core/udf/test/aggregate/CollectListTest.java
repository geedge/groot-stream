package com.geedgenetworks.core.udf.test.aggregate;


import com.geedgenetworks.common.config.Accumulator;
import com.geedgenetworks.core.udf.udaf.CollectList;
import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.connector.event.Event;
import org.junit.jupiter.api.Test;

import java.text.ParseException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CollectListTest {

    @Test
    public void test() throws ParseException {

        List<String> arr = List.of("192.168.1.1", "192.168.1.2", "192.168.1.3", "192.168.1.4");
        List<String> arr2 = List.of("192.168.1.5", "192.168.1.6", "192.168.1.3", "192.168.1.4");
        testMerge(arr,arr2);
        testGetResult(arr);

    }

    private void testMerge(List<String> arr,List<String> arr2) {

        UDFContext udfContext = new UDFContext();
        udfContext.setLookupFields(List.of("field"));
        udfContext.setOutputFields(Collections.singletonList("field_list"));
        CollectList collectList = new CollectList();
        Map<String, Object> metricsFields = new HashMap<>();
        Accumulator accumulator = new Accumulator();
        accumulator.setMetricsFields(metricsFields);
        collectList.open(udfContext);
        Accumulator result1 = getMiddleResult(udfContext,arr);
        Accumulator result2 = getMiddleResult(udfContext,arr2);
        Accumulator result = collectList.getResult(collectList.merge(result1,result2));
        List<String> vals = (List<String>) result.getMetricsFields().get("field_list");
        assertEquals(vals.size(),8);
        assertEquals("192.168.1.6",vals.get(5).toString());

    }
    private Accumulator getMiddleResult(UDFContext udfContext,List<String> arr) {


        CollectList collectList = new CollectList();
        Map<String, Object> metricsFields = new HashMap<>();
        Accumulator accumulator = new Accumulator();
        accumulator.setMetricsFields(metricsFields);
        collectList.open(udfContext);
        Accumulator agg = collectList.initAccumulator(accumulator);

        for (String o : arr) {
            Event event = new Event();
            Map<String, Object> extractedFields = new HashMap<>();
            extractedFields.put("field", o);
            event.setExtractedFields(extractedFields);
            agg = collectList.add(event, agg);

        }
        return agg;
    }
    private void testGetResult(List<String> arr) throws ParseException {

        UDFContext udfContext = new UDFContext();
        udfContext.setLookupFields(List.of("field"));
        udfContext.setOutputFields(Collections.singletonList("field_list"));
        CollectList collectList = new CollectList();
        Map<String, Object> metricsFields = new HashMap<>();
        Accumulator accumulator = new Accumulator();
        accumulator.setMetricsFields(metricsFields);
        collectList.open(udfContext);
        Accumulator agg = collectList.initAccumulator(accumulator);

        for (String o : arr) {
            Event event = new Event();
            Map<String, Object> extractedFields = new HashMap<>();
            extractedFields.put("field", o);
            event.setExtractedFields(extractedFields);
            agg = collectList.add(event, agg);

        }
        Accumulator result = collectList.getResult(agg);
        List<String> vals = (List<String>) result.getMetricsFields().get("field_list");
        assertEquals(vals.size(),4);
    }



}