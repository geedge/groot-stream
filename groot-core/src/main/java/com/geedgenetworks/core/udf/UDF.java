package com.geedgenetworks.core.udf;

import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;

import java.io.Serializable;

public interface UDF extends Serializable {

    void init(UDFContext udfContext) throws Exception;

    Event evaluate(Event event);

    String functionName();
}
