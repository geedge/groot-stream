package com.geedgenetworks.core.udf.udaf;


import com.geedgenetworks.common.config.Accumulator;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.api.common.udf.AggregateFunction;
import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.event.Event;


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
        acc.getMetricsFields().put(outputField, new HashSet<>());
        return acc;
    }

    @Override
    public Accumulator add(Event event, Accumulator acc) {
        Object valueObj = event.getExtractedFields().get(lookupField);
        if (valueObj != null) {
            Object object = valueObj;
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
        Object firstValueObj = firstAcc.getMetricsFields().get(outputField);
        Object secondValueObj = secondAcc.getMetricsFields().get(outputField);
        if (firstValueObj != null && secondValueObj != null) {
            Set<Object> firstValue = (Set<Object>) firstValueObj;
            Set<Object> secondValue = (Set<Object>) secondValueObj;
            firstValue.addAll(secondValue);
        } else if (firstValueObj == null && secondValueObj !=null) {
            Set<Object> secondValue = (Set<Object>)secondValueObj;
            firstAcc.getMetricsFields().put(outputField, secondValue);
        }
        return firstAcc;
    }
}
