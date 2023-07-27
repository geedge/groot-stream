package net.geedgenetworks.udf;

import net.geedgenetworks.pojo.GrootEvent;
import net.geedgenetworks.pojo.ProjectionFunction;

public class RenameFunction implements UdfFunction {
    private ProjectionFunction projection_function;

    public RenameFunction(ProjectionFunction projection_function) {
        this.projection_function = projection_function;
    }



    @Override
    public void udffunction(GrootEvent grootEvent) {

        grootEvent.getAuxiliaryFieldsAndValues().put(projection_function.getOutput_fields().get(0),grootEvent.getAuxiliaryFieldsAndValues().get(projection_function.getLookup_fields().get(0)));
        grootEvent.getAuxiliaryFieldsAndValues().remove(projection_function.getLookup_fields().get(0));
    }
}
