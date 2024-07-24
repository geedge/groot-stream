package com.geedgenetworks.core.udf.udaf;

import com.geedgenetworks.common.Accumulator;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.udf.AggregateFunction;
import com.geedgenetworks.common.udf.UDFContext;


public class LongCount implements AggregateFunction {
    private String outputField;


    @Override
    public Accumulator open(UDFContext udfContext,Accumulator acc){
        if(udfContext.getOutput_fields()==null ){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Missing required parameters");
        }
        outputField = udfContext.getOutput_fields().get(0);
        return acc;
    }


    @Override
    public Accumulator add(Event event, Accumulator acc) {

        acc.getMetricsFields().compute(outputField, (k, v) -> (v == null) ? 1L : (long)v + 1L);
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
    public void close() {

    }

}
