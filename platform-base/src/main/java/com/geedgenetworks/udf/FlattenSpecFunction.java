package com.geedgenetworks.udf;

import com.geedgenetworks.pojo.GrootEvent;
import com.geedgenetworks.pojo.ProjectionFunction;

public class FlattenSpecFunction implements UdfFunction {

    private ProjectionFunction projection_function;
    public FlattenSpecFunction(ProjectionFunction projection_function) {
        this.projection_function=projection_function;
    }

  /*  @Override
    public void udffunction(Map<String, Object> AuxiliaryFieldsAndValue) {

        JsonPathUtil.analysis(AuxiliaryFieldsAndValue.get(projection_function.getLookup_fields().get(0)).toString(), projection_function.getParameters().get("param").toString());

    }*/

    @Override
    public void udffunction(GrootEvent grootEvent) {

    }
}
