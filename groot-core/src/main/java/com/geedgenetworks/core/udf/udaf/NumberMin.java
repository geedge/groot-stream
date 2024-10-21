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

/**
 * Collects elements within a group and returns the list of aggregated objects
 */
public class NumberMin implements AggregateFunction {

    private String lookupField;
    private String outputField;


    @Override
    public void open(UDFContext udfContext) {
        if (udfContext.getLookup_fields() == null) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Missing required parameters");
        }
        this.lookupField = udfContext.getLookup_fields().get(0);
        if (udfContext.getOutput_fields() != null && !udfContext.getOutput_fields().isEmpty()) {
            this.outputField = udfContext.getOutput_fields().get(0);
        } else {
            outputField = lookupField;
        }
    }

    @Override
    public Accumulator initAccumulator(Accumulator acc) {
        return acc;
    }

    @Override
    public Accumulator add(Event event, Accumulator acc) {
        if (acc.getMetricsFields().get(outputField)==null && event.getExtractedFields().get(lookupField)!=null) {
            acc.getMetricsFields().put(outputField, event.getExtractedFields().get(lookupField));
        } else if (acc.getMetricsFields().get(outputField)!=null && event.getExtractedFields()!=null) {
            double value1 = Double.parseDouble(acc.getMetricsFields().get(outputField).toString());
            double value2 = Double.parseDouble(event.getExtractedFields().get(lookupField).toString());
            if (value1 > value2) {
                acc.getMetricsFields().put(outputField, event.getExtractedFields().get(lookupField));
            }
        }
        return acc;
    }

    @Override
    public String functionName() {
        return "NUMBER_MIN";
    }

    @Override
    public Accumulator getResult(Accumulator acc) {
        return acc;
    }

    @Override
    public Accumulator merge(Accumulator firstAcc, Accumulator secondAcc) {
        if (!firstAcc.getMetricsFields().containsKey(outputField) && secondAcc.getMetricsFields().containsKey(outputField)) {
            firstAcc.getMetricsFields().put(outputField, secondAcc.getMetricsFields().get(outputField));
        }
        else if(firstAcc.getMetricsFields().containsKey(outputField) && !secondAcc.getMetricsFields().containsKey(outputField)){
            firstAcc.getMetricsFields().put(outputField, firstAcc.getMetricsFields().get(outputField));
        }
        else if(firstAcc.getMetricsFields().containsKey(outputField) && secondAcc.getMetricsFields().containsKey(outputField)){
            double value1 = Double.parseDouble(firstAcc.getMetricsFields().get(outputField).toString());
            double value2 = Double.parseDouble(secondAcc.getMetricsFields().get(outputField).toString());
            if (value1 > value2) {
                firstAcc.getMetricsFields().put(outputField, secondAcc.getMetricsFields().get(outputField));
            }
        }
        return firstAcc;
    }
}
