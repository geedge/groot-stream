package com.geedgenetworks.core.udf.udaf;

import com.geedgenetworks.common.Accumulator;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.udf.AggregateFunction;
import com.geedgenetworks.common.udf.UDFContext;


public class NumberSum implements AggregateFunction {
    private String lookupField;
    private String outputField;


    @Override
    public void open(UDFContext udfContext) {
        if (udfContext.getLookup_fields() == null) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Missing required parameters");
        }
        lookupField = udfContext.getLookup_fields().get(0);
        if (udfContext.getOutput_fields() != null && !udfContext.getOutput_fields().isEmpty()) {
            outputField = udfContext.getOutput_fields().get(0);
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
        Object valueObj = event.getExtractedFields().get(lookupField);
        if (valueObj!=null) {
            Number val = (Number) valueObj;
            Number aggregate = (Number) acc.getMetricsFields().getOrDefault(outputField, 0L);
            if (aggregate instanceof Long && (val instanceof Integer || val instanceof Long)) {
                aggregate = aggregate.longValue() + val.longValue();
            } else if (aggregate instanceof Float || val instanceof Float) {
                aggregate = aggregate.floatValue() + val.floatValue();
            } else if (aggregate instanceof Double || val instanceof Double) {
                aggregate = aggregate.doubleValue() + val.doubleValue();
            }
            acc.getMetricsFields().put(outputField, aggregate);
        }
        return acc;
    }

    @Override
    public String functionName() {
        return "NUMBER_SUM";
    }

    @Override
    public Accumulator getResult(Accumulator acc) {
        return acc;
    }

    @Override
    public void close() {

    }

    @Override
    public Accumulator merge(Accumulator firstAcc, Accumulator secondAcc) {


        Object firstValueObj = firstAcc.getMetricsFields().get(outputField);
        Object secondValueObj = secondAcc.getMetricsFields().get(outputField);
        if (firstValueObj != null && secondValueObj != null) {

            Number firstValue = (Number) firstValueObj;
            Number secondValue = (Number) secondValueObj;
            if (firstValue instanceof Long && (secondValue instanceof Integer || secondValue instanceof Long)) {
                firstValue = firstValue.longValue() + secondValue.longValue();
            } else if (firstValue instanceof Float || secondValue instanceof Float) {
                firstValue = firstValue.floatValue() + secondValue.floatValue();
            } else if (firstValue instanceof Double || secondValue instanceof Double) {
                firstValue = firstValue.doubleValue() + secondValue.doubleValue();
            }
            firstAcc.getMetricsFields().put(outputField, firstValue);
        } else if (firstValueObj == null && secondValueObj != null) {
            Number secondValue = (Number) secondValueObj;
            firstAcc.getMetricsFields().put(outputField, secondValue);
        }
        return firstAcc;
    }
}
