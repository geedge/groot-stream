package com.geedgenetworks.core.udf;

import com.geedgenetworks.common.udf.ScalarFunction;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.udf.UDFContext;
import org.apache.flink.api.common.functions.RuntimeContext;


public class Drop implements ScalarFunction {

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {
    }

    @Override
    public Event evaluate(Event event) {
        event.setDropped(true);
        return event;
    }

    @Override
    public String functionName() {
        return "DROP";
    }

    @Override
    public void close() {

    }

}
