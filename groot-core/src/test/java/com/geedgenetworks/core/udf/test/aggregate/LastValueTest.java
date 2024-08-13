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
import com.geedgenetworks.core.udf.udaf.FirstValue;
import com.geedgenetworks.core.udf.udaf.LastValue;
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
        excute(arr);

    }

    private static void excute(List<String> arr) throws ParseException {

        UDFContext udfContext = new UDFContext();
        udfContext.setLookup_fields(List.of("field"));
        udfContext.setOutput_fields(Collections.singletonList("field_last"));
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