package com.geedgenetworks.core.udf;

import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.ProjectionFunction;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.AviatorEvaluatorInstance;
import com.googlecode.aviator.Expression;
import com.googlecode.aviator.Options;

import java.io.Serializable;
import java.util.Map;

import static com.geedgenetworks.core.udf.Drop.Aviator.AviatorExcute;

public class Drop implements Udf {

    @Override
    public void withParameters(ProjectionFunction projection_function) {
        Aviator.init(projection_function.getFilter());
    }

    @Override
    public Event evaluate(Event event) {
        Boolean result = AviatorExcute(event.getExtractedFields());
        event.setFinal(result);
        return event;
    }

    @Override
    public String functionName() {
        return "DROP";
    }

    public static class Aviator implements Serializable {
        private static Expression compiledExp;
        private static String expression;

        public static void init(String expr) {

            expression = expr;
            AviatorEvaluatorInstance instance = AviatorEvaluator.getInstance();
            instance.setCachedExpressionByDefault(true);
            instance.setOption(Options.OPTIMIZE_LEVEL, AviatorEvaluator.EVAL);
            instance.setFunctionMissing(null);
            compiledExp = instance.compile(expression, true);
        }

        public static Boolean AviatorExcute(Map<String, Object> map) {

            Boolean bool = true;
            if (!"".equals(expression)) {
                bool = (Boolean) compiledExp.execute(compiledExp.newEnv("map", map));
            }
            return bool;
        }
    }
}
