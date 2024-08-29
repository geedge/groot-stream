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
import com.geedgenetworks.core.udf.udaf.CollectList;
import com.geedgenetworks.core.udf.udaf.CollectSet;
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
        udfContext.setLookup_fields(List.of("field"));
        udfContext.setOutput_fields(Collections.singletonList("field_list"));
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
        return collectSet.getMiddleResult(agg);
    }
    private static void testGetResult(List<String> arr) throws ParseException {

        UDFContext udfContext = new UDFContext();
        udfContext.setLookup_fields(List.of("field"));
        udfContext.setOutput_fields(Collections.singletonList("field_list"));
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