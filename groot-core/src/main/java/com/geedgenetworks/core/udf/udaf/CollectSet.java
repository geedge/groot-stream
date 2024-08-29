package com.geedgenetworks.core.udf.udaf;


import com.geedgenetworks.common.Accumulator;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.udf.AggregateFunction;
import com.geedgenetworks.common.udf.UDFContext;

import java.util.HashSet;
import java.util.Set;

/**
 * Collects elements within a group and returns the list of aggregated objects
 */
public class CollectSet implements AggregateFunction {

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
        acc.getMetricsFields().put(outputField, new HashSet<>());
        return acc;
    }

    @Override
    public Accumulator add(Event event, Accumulator acc) {
        if (event.getExtractedFields().containsKey(lookupField)) {
            Object object = event.getExtractedFields().get(lookupField);
            Set<Object> aggregate = (Set<Object>) acc.getMetricsFields().get(outputField);
            aggregate.add(object);
            acc.getMetricsFields().put(outputField, aggregate);
        }
        return acc;
    }

    @Override
    public String functionName() {
        return "COLLECT_SET";
    }

    @Override
    public Accumulator getResult(Accumulator acc) {
        return acc;
    }

    @Override
    public Accumulator merge(Accumulator firstAcc, Accumulator secondAcc) {
        if (firstAcc.getMetricsFields().containsKey(outputField) && secondAcc.getMetricsFields().containsKey(outputField)) {
            Set<Object> firstValue = (Set<Object>) firstAcc.getMetricsFields().get(outputField);
            Set<Object> secondValue = (Set<Object>) secondAcc.getMetricsFields().get(outputField);
            firstValue.addAll(secondValue);
        } else if (!firstAcc.getMetricsFields().containsKey(outputField) && secondAcc.getMetricsFields().containsKey(outputField)) {
            Set<Object> secondValue = (Set<Object>) secondAcc.getMetricsFields().get(outputField);
            firstAcc.getMetricsFields().put(outputField, secondValue);
        }
        return firstAcc;
    }
}
