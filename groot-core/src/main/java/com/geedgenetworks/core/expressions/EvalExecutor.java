package com.geedgenetworks.core.expressions;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.googlecode.aviator.exception.ExpressionRuntimeException;
import com.googlecode.aviator.exception.ExpressionSyntaxErrorException;

import java.util.Map;

/**
 * Eval Function Executor.
 *
 * @author chaoc
 * @since 1.5
 */
public final class EvalExecutor implements Calc {

    private final Expression script;

    public EvalExecutor(String script) {
        try {
            AviatorEvaluator.validate(script);
            this.script =
                    AviatorEvaluator.getInstance().compile(script);
        } catch (ExpressionSyntaxErrorException
                 | ExpressionRuntimeException e) {
            throw new SyntaxErrorException("Eval script syntax error. ", e);
        }
    }

    @Override
    public Object eval(final Map<String, Object> data) {
        return script.execute(data);
    }
}
