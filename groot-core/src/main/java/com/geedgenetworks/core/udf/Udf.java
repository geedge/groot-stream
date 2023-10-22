package com.geedgenetworks.core.udf;

import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.ProjectionFunction;

import java.io.Serializable;

public interface Udf extends Serializable {

    void withParameters(ProjectionFunction projection_function) throws Exception;

    Event evaluate(Event event);

    String functionName();
}
