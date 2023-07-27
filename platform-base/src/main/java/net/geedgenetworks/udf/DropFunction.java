package net.geedgenetworks.udf;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.AviatorEvaluatorInstance;
import com.googlecode.aviator.Expression;
import com.googlecode.aviator.Options;
import net.geedgenetworks.pojo.GrootEvent;
import net.geedgenetworks.pojo.ProjectionFunction;

public class DropFunction implements UdfFunction {
    private ProjectionFunction projection_function;
    private Expression compiledExp;

    public DropFunction(ProjectionFunction projection_function) {
        this.projection_function=projection_function;

        AviatorEvaluatorInstance instance = AviatorEvaluator.getInstance();
        instance.setCachedExpressionByDefault(true);
        instance.setOption(Options.OPTIMIZE_LEVEL, AviatorEvaluator.EVAL);
        instance.setFunctionMissing(null);
        String expression =  projection_function.getFilter();
        this.compiledExp = instance.compile(expression, true);
    }


    @Override
    public void udffunction(GrootEvent grootEvent) {

        Boolean result = (Boolean) compiledExp.execute(compiledExp.newEnv("map", grootEvent.getAuxiliaryFieldsAndValues()));
        grootEvent.setFlag(!result);
    }
}
