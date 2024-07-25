package com.geedgenetworks.common.udf;

import com.geedgenetworks.common.Accumulator;
import com.geedgenetworks.common.Event;

import java.io.Serializable;

public interface AggregateFunction extends Serializable {

    Accumulator open(UDFContext udfContext,Accumulator acc);

    Accumulator add(Event val, Accumulator acc);

    String functionName();

    Accumulator getResult(Accumulator acc);

    void close();

}
