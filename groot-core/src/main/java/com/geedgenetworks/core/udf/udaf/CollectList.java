/**
 * Copyright 2017 Hortonworks.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package com.geedgenetworks.core.udf.udaf;


import com.geedgenetworks.common.Accumulator;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.udf.AggregateFunction;
import com.geedgenetworks.common.udf.UDFContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Collects elements within a group and returns the list of aggregated objects
 */
public class CollectList implements AggregateFunction {

    private String lookupField;
    private String outputField;

    @Override
    public void open(UDFContext udfContext) {
        if (udfContext.getLookupFields() == null) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Missing required parameters");
        }
        this.lookupField = udfContext.getLookupFields().get(0);
        if (udfContext.getOutputFields() != null && !udfContext.getOutputFields().isEmpty()) {
            this.outputField = udfContext.getOutputFields().get(0);
        } else {
            outputField = lookupField;
        }

    }

    @Override
    public Accumulator initAccumulator(Accumulator acc) {
        acc.getMetricsFields().put(outputField, new ArrayList<>());
        return acc;
    }

    @Override
    public Accumulator add(Event event, Accumulator acc) {
        Object valueObj = event.getExtractedFields().get(lookupField);
        if (valueObj != null) {
            Object object = valueObj;
            List<Object> aggregate = (List<Object>) acc.getMetricsFields().get(outputField);
            aggregate.add(object);
            acc.getMetricsFields().put(outputField, aggregate);
        }
        return acc;
    }

    @Override
    public String functionName() {
        return "COLLECT_LIST";
    }

    @Override
    public Accumulator getResult(Accumulator acc) {
        return acc;
    }

    @Override
    public Accumulator merge(Accumulator firstAcc, Accumulator secondAcc) {
        Object firstValueObj = firstAcc.getMetricsFields().get(outputField);
        Object secondValueObj = secondAcc.getMetricsFields().get(outputField);
        if (firstValueObj != null && secondValueObj != null) {
            List<Object> firstValue = (List<Object>) firstValueObj;
            List<Object> secondValue = (List<Object>) secondValueObj;
            firstValue.addAll(secondValue);
        } else if (firstValueObj == null && secondValueObj != null) {
            List<Object> secondValue = (List<Object>) secondValueObj;
            firstAcc.getMetricsFields().put(outputField, secondValue);
        }
        return firstAcc;
    }

}
