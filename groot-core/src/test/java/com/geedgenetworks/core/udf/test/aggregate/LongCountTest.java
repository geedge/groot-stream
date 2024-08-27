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
import com.geedgenetworks.core.udf.udaf.LongCount;
import com.geedgenetworks.core.udf.udaf.NumberSum;
import com.ibm.icu.text.NumberFormat;
import org.junit.jupiter.api.Test;

import java.text.ParseException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class LongCountTest {

    @Test
    public void test() throws ParseException {

        Long[] longArr = new Long[]{1L, 2L, 3L, 4L};
        excute(longArr);
    }

    private static void excute(Number[] arr) throws ParseException {

        UDFContext udfContext = new UDFContext();
        udfContext.setOutput_fields(Collections.singletonList("count"));
        LongCount longCount = new LongCount();
        Map<String, Object> metricsFields = new HashMap<>();
        Accumulator accumulator = new Accumulator();
        accumulator.setMetricsFields(metricsFields);
        longCount.open(udfContext);
        Accumulator agg = longCount.initAccumulator(accumulator);
        for (Number o : arr) {
            Event event = new Event();
            Map<String, Object> extractedFields = new HashMap<>();
            extractedFields.put("field", o);
            event.setExtractedFields(extractedFields);
            agg = longCount.add(event, agg);

        }
        Accumulator result = longCount.getResult(agg);
        assertEquals(Integer.parseInt((result.getMetricsFields().get("count").toString())),4);
    }




}