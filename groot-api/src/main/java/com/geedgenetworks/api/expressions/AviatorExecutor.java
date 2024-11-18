package com.geedgenetworks.api.expressions;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.AviatorEvaluatorInstance;
import com.googlecode.aviator.Expression;
import com.googlecode.aviator.Options;
import com.googlecode.aviator.exception.ExpressionRuntimeException;
import com.googlecode.aviator.exception.ExpressionSyntaxErrorException;

import java.util.Map;

/**
 * Eval Function Executor.
 *
 * @author chaoc
 * @since 1.5
 */
public final class AviatorExecutor implements Calc {

    private final Expression script;

    public AviatorExecutor(String script) {
        try {
            AviatorEvaluator.validate(script);
            AviatorEvaluatorInstance instance = AviatorEvaluator.getInstance();
            instance.setCachedExpressionByDefault(true);
            instance.setOption(Options.OPTIMIZE_LEVEL, AviatorEvaluator.EVAL);
            instance.setFunctionMissing(null);
            this.script = instance.compile(script,true);
        } catch (ExpressionSyntaxErrorException
                 | ExpressionRuntimeException e) {
            throw new SyntaxErrorException("Eval script syntax error. ", e);
        }
    }

    @Override
    public Object eval(final Map<String, Object> data) {
        return script.execute(data);
    }

    @Override
    public Boolean filter(Map<String, Object> data) {

        boolean result;
        Object object = script.execute(data);
        if (object != null) {
            result = (Boolean) object;
        } else {
            throw new ExpressionRuntimeException();
        }
        return result;
    }


}
