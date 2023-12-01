package com.geedgenetworks.core.udf;

import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.AviatorEvaluatorInstance;
import com.googlecode.aviator.Expression;
import com.googlecode.aviator.Options;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.io.Serializable;
import java.util.Map;

import static com.geedgenetworks.core.udf.Drop.Aviator.AviatorExecute;

public class Drop implements UDF {

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {
        Aviator.init(udfContext.getFilter());
    }

    @Override
    public Event evaluate(Event event) {
        Boolean result = AviatorExecute(event.getExtractedFields());
        event.setFinal(result);
        return event;
    }

    @Override
    public String functionName() {
        return "DROP";
    }

    @Override
    public void close() {

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

        public static Boolean AviatorExecute(Map<String, Object> map) {

            Boolean bool = true;
            if (!"".equals(expression)) {
                bool = (Boolean) compiledExp.execute(compiledExp.newEnv("map", map));
            }
            return bool;
        }
    }
}
