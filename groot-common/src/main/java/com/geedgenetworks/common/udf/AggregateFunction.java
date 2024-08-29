package com.geedgenetworks.common.udf;

import com.geedgenetworks.common.Accumulator;
import com.geedgenetworks.common.Event;

import java.io.Serializable;

public interface AggregateFunction extends Serializable {

    void open(UDFContext udfContext);
    Accumulator initAccumulator(Accumulator acc);
    Accumulator add(Event val, Accumulator acc);
    String functionName();
    default Accumulator getMiddleResult(Accumulator acc){return acc;}
    Accumulator getResult(Accumulator acc);
    Accumulator merge(Accumulator a, Accumulator b);
    default void close(){};
}
