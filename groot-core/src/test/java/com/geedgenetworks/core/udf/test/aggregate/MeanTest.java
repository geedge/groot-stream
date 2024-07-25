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
import com.geedgenetworks.core.udf.udaf.Mean;
import com.geedgenetworks.core.udf.udaf.NumberSum;
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
    public void testNumberSumTest() throws ParseException {

        Integer[] intArr1 = new Integer[]{1, 2, 3, 4};
        Integer[] intArr2 = new Integer[]{1, 6, 3};
        excute(intArr1, 0);
        excute2(intArr2, 2);
    }

    private static void excute(Number[] arr,int precision) throws ParseException {

        UDFContext udfContext = new UDFContext();
        udfContext.setLookup_fields(List.of("field"));
        udfContext.setOutput_fields(Collections.singletonList("field_mean"));
        udfContext.setParameters(new HashMap<>());
        udfContext.getParameters().put("precision", precision);
        Mean mean = new Mean();
        Map<String, Object> metricsFields = new HashMap<>();
        Accumulator accumulator = new Accumulator();
        accumulator.setMetricsFields(metricsFields);
        Accumulator agg = mean.open(udfContext,accumulator);

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

    private static void excute2(Number[] arr,int precision) throws ParseException {

        UDFContext udfContext = new UDFContext();
        udfContext.setLookup_fields(List.of("field"));
        udfContext.setOutput_fields(Collections.singletonList("field_mean"));
        udfContext.setParameters(new HashMap<>());
        udfContext.getParameters().put("precision", precision);
        Mean mean = new Mean();
        Map<String, Object> metricsFields = new HashMap<>();
        Accumulator accumulator = new Accumulator();
        accumulator.setMetricsFields(metricsFields);
        Accumulator agg = mean.open(udfContext,accumulator);

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


}