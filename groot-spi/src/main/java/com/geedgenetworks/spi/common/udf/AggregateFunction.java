package com.geedgenetworks.spi.common.udf;

import com.geedgenetworks.common.config.Accumulator;
import com.geedgenetworks.spi.table.event.Event;

import java.io.Serializable;

public interface AggregateFunction extends Serializable {

    void open(UDFContext udfContext);
    Accumulator initAccumulator(Accumulator acc);
    Accumulator add(Event val, Accumulator acc);
    String functionName();
    Accumulator getResult(Accumulator acc);
    Accumulator merge(Accumulator a, Accumulator b);
    default void close(){};
}
