/**
  * Copyright 2017 Hortonworks.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at

  *   http://www.apache.org/licenses/LICENSE-2.0

  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
 **/
package com.geedgenetworks.core.udf.test.aggregate;


import com.geedgenetworks.common.Accumulator;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.udf.UDFContext;
import com.geedgenetworks.core.udf.udaf.NumberMax;
import com.geedgenetworks.core.udf.udaf.NumberMin;
import com.ibm.icu.text.NumberFormat;
import org.junit.jupiter.api.Test;

import java.text.ParseException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class NumberMinTest {

    @Test
    public void test() throws ParseException {

        Integer[] intArr = new Integer[]{1, 2, 3, 4};
        Long[] longArr = new Long[]{1L, 2L, 3L, 4L};
        Float[] floatArr = new Float[]{1.0f, 2.0f, 3.0f, 4.0f};
        Double[] doubleArr = new Double[]{1.0, 2.0, 3.0, 4.0};
        excute(floatArr, Float.class);
        excute(doubleArr, Double.class);
        excute(intArr, Integer.class);
        excute(longArr, Long.class);
        testMerge(intArr,floatArr);

    }
    private void testMerge(Number[] arr,Number[] arr2) {

        UDFContext udfContext = new UDFContext();
        udfContext.setLookup_fields(List.of("field"));
        udfContext.setOutput_fields(Collections.singletonList("field_min"));
        NumberMin numberMin = new NumberMin();
        Map<String, Object> metricsFields = new HashMap<>();
        Accumulator accumulator = new Accumulator();
        accumulator.setMetricsFields(metricsFields);
        numberMin.open(udfContext);
        Accumulator result1 = getMiddleResult(udfContext,arr);
        Accumulator result2 = getMiddleResult(udfContext,arr2);
        Accumulator result = numberMin.getResult(numberMin.merge(result1,result2));
        assertEquals(Float.parseFloat((result.getMetricsFields().get("field_min").toString())),1);

    }
    private Accumulator getMiddleResult(UDFContext udfContext,Number[] arr) {


        NumberMin numberMin = new NumberMin();
        Map<String, Object> metricsFields = new HashMap<>();
        Accumulator accumulator = new Accumulator();
        accumulator.setMetricsFields(metricsFields);
        numberMin.open(udfContext);
        Accumulator agg = numberMin.initAccumulator(accumulator);
        for (Number o : arr) {
            Event event = new Event();
            Map<String, Object> extractedFields = new HashMap<>();
            extractedFields.put("field", o);
            event.setExtractedFields(extractedFields);
            agg = numberMin.add(event, agg);

        }
        return agg;
    }
    private static void excute(Number[] arr, Class<? extends Number> clazz) throws ParseException {

        UDFContext udfContext = new UDFContext();
        udfContext.setLookup_fields(List.of("field"));
        udfContext.setOutput_fields(Collections.singletonList("field_min"));
        NumberMin numberMin = new NumberMin();
        Map<String, Object> metricsFields = new HashMap<>();
        Accumulator accumulator = new Accumulator();
        accumulator.setMetricsFields(metricsFields);
        numberMin.open(udfContext);
        Accumulator agg = numberMin.initAccumulator(accumulator);
        for (Number o : arr) {
            Event event = new Event();
            Map<String, Object> extractedFields = new HashMap<>();
            extractedFields.put("field", o);
            event.setExtractedFields(extractedFields);
            agg = numberMin.add(event, agg);

        }
        Accumulator result = numberMin.getResult(agg);
        assertEquals(clazz, result.getMetricsFields().get("field_min").getClass());
        assertEquals(NumberFormat.getInstance().parse((result.getMetricsFields().get("field_min").toString())),NumberFormat.getInstance().parse("1"));
    }




}