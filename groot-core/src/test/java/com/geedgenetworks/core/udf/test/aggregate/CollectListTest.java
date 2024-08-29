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
        udfContext.setLookup_fields(List.of("field"));
        udfContext.setOutput_fields(Collections.singletonList("field_list"));
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
        return collectList.getMiddleResult(agg);
    }
    private void testGetResult(List<String> arr) throws ParseException {

        UDFContext udfContext = new UDFContext();
        udfContext.setLookup_fields(List.of("field"));
        udfContext.setOutput_fields(Collections.singletonList("field_list"));
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