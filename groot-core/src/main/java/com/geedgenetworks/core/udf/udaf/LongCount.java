package com.geedgenetworks.core.udf.udaf;

import com.geedgenetworks.common.config.Accumulator;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.api.common.udf.AggregateFunction;
import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.connector.event.Event;


public class LongCount implements AggregateFunction {
    private String outputField;


    @Override
    public void open(UDFContext udfContext) {
        if (udfContext.getOutputFields() == null) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Missing required parameters");
        }
        outputField = udfContext.getOutputFields().get(0);
    }

    @Override
    public Accumulator initAccumulator(Accumulator acc) {
        return acc;
    }

    @Override
    public Accumulator add(Event event, Accumulator acc) {

        acc.getMetricsFields().compute(outputField, (k, v) -> (v == null) ? 1L : (long) v + 1L);
        return acc;
    }

    @Override
    public String functionName() {
        return "LONG_COUNT";
    }

    @Override
    public Accumulator getResult(Accumulator acc) {
        return acc;
    }

    @Override
    public Accumulator merge(Accumulator firstAcc, Accumulator secondAcc) {

        Object firstValueObj = firstAcc.getMetricsFields().get(outputField);
        Object secondValueObj = secondAcc.getMetricsFields().get(outputField);
        if (firstValueObj!=null && secondValueObj!=null) {
            long firstValue = (long) firstValueObj;
            long secondValue = (long) secondValueObj;
            firstValue = firstValue + secondValue;
            firstAcc.getMetricsFields().put(outputField, firstValue);
        } else if (firstValueObj==null && secondValueObj!=null) {
            Number secondValue = (Number) secondValueObj;
            firstAcc.getMetricsFields().put(outputField, secondValue);
        }
        return firstAcc;
    }
}
