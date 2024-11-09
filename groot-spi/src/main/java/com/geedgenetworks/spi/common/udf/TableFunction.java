package com.geedgenetworks.spi.common.udf;

import com.geedgenetworks.spi.table.event.Event;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.io.Serializable;
import java.util.List;

public interface TableFunction extends Serializable {

    void open(RuntimeContext runtimeContext, UDFContext udfContext);

    List<Event> evaluate(Event event);

    String functionName();

    void close();

}
