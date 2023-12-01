package com.geedgenetworks.core.udf;

import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.io.Serializable;

public interface UDF extends Serializable {

    void open(RuntimeContext runtimeContext, UDFContext udfContext);

    Event evaluate(Event event);

    String functionName();

    void close();

}
