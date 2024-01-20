package com.geedgenetworks.common.udf;
import com.geedgenetworks.common.Event;
import org.apache.flink.api.common.functions.RuntimeContext;
import java.io.Serializable;

public interface UDF extends Serializable {

    void open(RuntimeContext runtimeContext, UDFContext udfContext);

    Event evaluate(Event event);

    String functionName();

    void close();

}
