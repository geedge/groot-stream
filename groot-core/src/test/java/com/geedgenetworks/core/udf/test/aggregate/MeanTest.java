package com.geedgenetworks.core.udf.test.aggregate;


import com.geedgenetworks.common.config.Accumulator;
import com.geedgenetworks.core.udf.udaf.Mean;
import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.connector.event.Event;
import com.ibm.icu.text.NumberFormat;
import org.junit.jupiter.api.Test;

import java.text.ParseException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class MeanTest {

    @Test
    public void test() throws ParseException {

        Integer[] intArr1 = new Integer[]{1, 2, 3, 4};
        Integer[] intArr2 = new Integer[]{1, 6, 3};
        testInt(intArr1, 0);
        testDouble(intArr2, 2);
        testNoPrecision(intArr1);
        testMerge(intArr1,intArr2,2);
    }
    private void testMerge(Number[] arr1,Number[] arr2,int precision) throws ParseException {
        UDFContext udfContext = new UDFContext();
        udfContext.setLookupFields(List.of("field"));
        udfContext.setOutputFields(Collections.singletonList("field_mean"));
        udfContext.setParameters(new HashMap<>());
        udfContext.getParameters().put("precision", precision);
        Mean mean = new Mean();
        Map<String, Object> metricsFields = new HashMap<>();
        Accumulator accumulator = new Accumulator();
        accumulator.setMetricsFields(metricsFields);
        mean.open(udfContext);
        Accumulator result1 = getMiddleResult(arr1,precision);
        Accumulator result2 = getMiddleResult(arr2,precision);
        Accumulator result = mean.getResult(mean.merge(result1,result2));
        assertEquals(NumberFormat.getInstance().parse((result.getMetricsFields().get("field_mean").toString())),NumberFormat.getInstance().parse("2.86"));
    }
    private Accumulator getMiddleResult(Number[] arr,int precision) throws ParseException {
        UDFContext udfContext = new UDFContext();
        udfContext.setLookupFields(List.of("field"));
        udfContext.setOutputFields(Collections.singletonList("field_mean"));
        udfContext.setParameters(new HashMap<>());
        udfContext.getParameters().put("precision", precision);
        Mean mean = new Mean();
        Map<String, Object> metricsFields = new HashMap<>();
        Accumulator accumulator = new Accumulator();
        accumulator.setMetricsFields(metricsFields);
        mean.open(udfContext);
        Accumulator agg = mean.initAccumulator(accumulator);
        for (Number o : arr) {
            Event event = new Event();
            Map<String, Object> extractedFields = new HashMap<>();
            extractedFields.put("field", o);
            event.setExtractedFields(extractedFields);
            agg = mean.add(event, agg);

        }
        return agg;
    }


        private void testInt(Number[] arr,int precision) throws ParseException {

        UDFContext udfContext = new UDFContext();
        udfContext.setLookupFields(List.of("field"));
        udfContext.setOutputFields(Collections.singletonList("field_mean"));
        udfContext.setParameters(new HashMap<>());
        udfContext.getParameters().put("precision", precision);
        Mean mean = new Mean();
        Map<String, Object> metricsFields = new HashMap<>();
        Accumulator accumulator = new Accumulator();
        accumulator.setMetricsFields(metricsFields);
        mean.open(udfContext);
        Accumulator agg = mean.initAccumulator(accumulator);
        for (Number o : arr) {
            Event event = new Event();
            Map<String, Object> extractedFields = new HashMap<>();
            extractedFields.put("field", o);
            event.setExtractedFields(extractedFields);
            agg = mean.add(event, agg);

        }

        Accumulator result = mean.getResult(agg);
        assertEquals(NumberFormat.getInstance().parse((result.getMetricsFields().get("field_mean").toString())),NumberFormat.getInstance().parse("2"));
    }

    private  void testDouble(Number[] arr,int precision) throws ParseException {

        UDFContext udfContext = new UDFContext();
        udfContext.setLookupFields(List.of("field"));
        udfContext.setOutputFields(Collections.singletonList("field_mean"));
        udfContext.setParameters(new HashMap<>());
        udfContext.getParameters().put("precision", precision);
        Mean mean = new Mean();
        Map<String, Object> metricsFields = new HashMap<>();
        Accumulator accumulator = new Accumulator();
        accumulator.setMetricsFields(metricsFields);
        mean.open(udfContext);
        Accumulator agg = mean.initAccumulator(accumulator);
        for (Number o : arr) {
            Event event = new Event();
            Map<String, Object> extractedFields = new HashMap<>();
            extractedFields.put("field", o);
            event.setExtractedFields(extractedFields);
            agg = mean.add(event, agg);

        }
        Accumulator result = mean.getResult(agg);
        assertEquals(NumberFormat.getInstance().parse((result.getMetricsFields().get("field_mean").toString())),NumberFormat.getInstance().parse("3.33"));
    }

    private void testNoPrecision(Number[] arr) throws ParseException {

        UDFContext udfContext = new UDFContext();
        udfContext.setLookupFields(List.of("field"));
        udfContext.setOutputFields(Collections.singletonList("field_mean"));
        Mean mean = new Mean();
        Map<String, Object> metricsFields = new HashMap<>();
        Accumulator accumulator = new Accumulator();
        accumulator.setMetricsFields(metricsFields);
        mean.open(udfContext);
        Accumulator agg = mean.initAccumulator(accumulator);
        for (Number o : arr) {
            Event event = new Event();
            Map<String, Object> extractedFields = new HashMap<>();
            extractedFields.put("field", o);
            event.setExtractedFields(extractedFields);
            agg = mean.add(event, agg);

        }
        Accumulator result = mean.getResult(agg);
        assertEquals(NumberFormat.getInstance().parse((result.getMetricsFields().get("field_mean").toString())),NumberFormat.getInstance().parse("2.5"));
    }
}