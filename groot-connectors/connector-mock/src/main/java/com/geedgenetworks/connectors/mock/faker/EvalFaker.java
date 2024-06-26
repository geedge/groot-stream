package com.geedgenetworks.connectors.mock.faker;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;

import java.util.Map;

public class EvalFaker extends Faker<Object> {
    private final String expression;
    private Expression script;

    public EvalFaker(String expression) {
        this.expression = expression;
    }

    @Override
    public void init(int instanceCount, int instanceIndex) throws Exception {
        script = AviatorEvaluator.compile(expression, true);
    }

    @Override
    public Object geneValue() throws Exception {
        throw new UnsupportedOperationException();
    }

    public Object geneValue(Map<String, Object> params) throws Exception {
        return script.execute(params);
    }

    @Override
    public boolean isComputeFaker() {
        return true;
    }
}
