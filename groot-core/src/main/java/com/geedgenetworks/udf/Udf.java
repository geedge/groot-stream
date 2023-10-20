package com.geedgenetworks.udf;

import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.pojo.ProjectionFunction;

import java.io.Serializable;

public interface Udf extends Serializable {

    void withParameters(ProjectionFunction projection_function) throws Exception;

    Event evaluate(Event event);

    String functionName();
}
