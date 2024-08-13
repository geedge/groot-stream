package com.geedgenetworks.common.udf;

import com.geedgenetworks.common.Event;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.List;

public interface TableFunction extends Serializable {

    void open(RuntimeContext runtimeContext, UDFContext udfContext);

    List<Event> evaluate(Event event);

    String functionName();

    void close();

}
