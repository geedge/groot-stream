package com.geedgenetworks.udf;

import com.geedgenetworks.pojo.GrootEvent;
import com.geedgenetworks.pojo.ProjectionFunction;

public class SetValueFunction implements UdfFunction {


    private ProjectionFunction projection_function;
    public SetValueFunction(ProjectionFunction projection_function) {
        this.projection_function=projection_function;
    }
   /* @Override
    public void udffunction(Map<String, Object> AuxiliaryFieldsAndValue) {


        AuxiliaryFieldsAndValue.put(projection_function.getOutput_fields().get(0),projection_function.getParameters().get("param"));

    }*/

    @Override
    public void udffunction(GrootEvent grootEvent) {

    }
}
