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


import com.geedgenetworks.common.config.Accumulator;
import com.geedgenetworks.core.udf.udaf.NumberSum;
import com.geedgenetworks.spi.common.udf.UDFContext;
import com.geedgenetworks.spi.table.event.Event;
import com.ibm.icu.text.NumberFormat;
import org.junit.jupiter.api.Test;

import java.text.ParseException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class NumberSumTest {

    @Test
    public void test() throws ParseException {

        Integer[] intArr = new Integer[]{1, 2, 3, 4};
        Long[] longArr = new Long[]{1L, 2L, 3L, 4L};
        Float[] floatArr = new Float[]{1.0f, 2.0f, 3.0f, 4.0f};
        Double[] doubleArr = new Double[]{1.0, 2.0, 3.0, 4.0};
        excute(floatArr, Float.class);
        excute(doubleArr, Double.class);
        excute(intArr, Long.class);
        excute(longArr, Long.class);
        testMerge(intArr,floatArr);

    }
    private void testMerge(Number[] arr,Number[] arr2) {

        UDFContext udfContext = new UDFContext();
        udfContext.setLookupFields(List.of("field"));
        udfContext.setOutputFields(Collections.singletonList("field_sum"));
        NumberSum numberSum = new NumberSum();
        Map<String, Object> metricsFields = new HashMap<>();
        Accumulator accumulator = new Accumulator();
        accumulator.setMetricsFields(metricsFields);
        numberSum.open(udfContext);
        Accumulator result1 = getMiddleResult(udfContext,arr);
        Accumulator result2 = getMiddleResult(udfContext,arr2);
        Accumulator result = numberSum.getResult(numberSum.merge(result1,result2));
        assertEquals(Float.parseFloat((result.getMetricsFields().get("field_sum").toString())),20.0f);

    }
    private Accumulator getMiddleResult(UDFContext udfContext,Number[] arr) {


        NumberSum numberSum = new NumberSum();
        Map<String, Object> metricsFields = new HashMap<>();
        Accumulator accumulator = new Accumulator();
        accumulator.setMetricsFields(metricsFields);
        numberSum.open(udfContext);
        Accumulator agg = numberSum.initAccumulator(accumulator);
        for (Number o : arr) {
            Event event = new Event();
            Map<String, Object> extractedFields = new HashMap<>();
            extractedFields.put("field", o);
            event.setExtractedFields(extractedFields);
            agg = numberSum.add(event, agg);

        }
        return agg;
    }
    private static void excute(Number[] arr, Class<? extends Number> clazz) throws ParseException {

        UDFContext udfContext = new UDFContext();
        udfContext.setLookupFields(List.of("field"));
        udfContext.setOutputFields(Collections.singletonList("field_sum"));
        NumberSum numberSum = new NumberSum();
        Map<String, Object> metricsFields = new HashMap<>();
        Accumulator accumulator = new Accumulator();
        accumulator.setMetricsFields(metricsFields);
        numberSum.open(udfContext);
        Accumulator agg = numberSum.initAccumulator(accumulator);
        for (Number o : arr) {
            Event event = new Event();
            Map<String, Object> extractedFields = new HashMap<>();
            extractedFields.put("field", o);
            event.setExtractedFields(extractedFields);
            agg = numberSum.add(event, agg);

        }
        Accumulator result = numberSum.getResult(agg);
        assertEquals(clazz, result.getMetricsFields().get("field_sum").getClass());
        assertEquals(NumberFormat.getInstance().parse((result.getMetricsFields().get("field_sum").toString())),NumberFormat.getInstance().parse("10"));
    }




}