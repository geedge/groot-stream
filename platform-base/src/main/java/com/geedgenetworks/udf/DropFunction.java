package com.geedgenetworks.udf;

import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.pojo.ProjectionFunction;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.AviatorEvaluatorInstance;
import com.googlecode.aviator.Expression;
import com.googlecode.aviator.Options;

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
    public void udffunction(Event event) {

        Boolean result = (Boolean) compiledExp.execute(compiledExp.newEnv("map", event.getExtractedFields()));
        event.setFinal(!result);
    }
}
