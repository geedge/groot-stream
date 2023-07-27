package net.geedgenetworks.udf;

import com.zdjizhi.utils.FormatUtils;
import net.geedgenetworks.pojo.GrootEvent;
import net.geedgenetworks.pojo.ProjectionFunction;

public class TopDomainFunction implements UdfFunction {


    private ProjectionFunction projection_function;
    public TopDomainFunction(ProjectionFunction projection_function) {
        this.projection_function=projection_function;
    }

    @Override
    public void udffunction(GrootEvent grootEvent) {
        String  topDomain = FormatUtils.getTopPrivateDomain((String) grootEvent.getAuxiliaryFieldsAndValues().get(projection_function.getLookup_fields().get(0)));
        grootEvent.getAuxiliaryFieldsAndValues().put(projection_function.getOutput_fields().get(0),topDomain);
    }
}
