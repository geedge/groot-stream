package com.geedgenetworks.core.udf;

import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;
import org.apache.flink.api.common.functions.RuntimeContext;


public class Drop implements UDF {

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
