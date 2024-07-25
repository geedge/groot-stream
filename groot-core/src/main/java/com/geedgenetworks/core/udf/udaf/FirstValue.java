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
public class FirstValue implements AggregateFunction {

    private String lookupField;
    private String outputField;


    @Override
    public Accumulator open(UDFContext udfContext,Accumulator acc) {
        if(udfContext.getLookup_fields()==null ){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Missing required parameters");
        }
        this.lookupField  = udfContext.getLookup_fields().get(0);
        if(udfContext.getOutput_fields()!=null && !udfContext.getOutput_fields().isEmpty()) {
            this.outputField = udfContext.getOutput_fields().get(0);
        }
        else {
            outputField = lookupField;
        }
        return acc;
    }


    @Override
    public Accumulator add(Event event, Accumulator acc) {
        if(!acc.getMetricsFields().containsKey(outputField) && event.getExtractedFields().containsKey(lookupField)){
            acc.getMetricsFields().put(outputField, event.getExtractedFields().get(lookupField));
        }
        return acc;
    }

    @Override
    public String functionName() {
        return "FIRST_VALUE";
    }

    @Override
    public Accumulator getResult(Accumulator acc) {
        return acc;
    }

    @Override
    public void close() {

    }
}
