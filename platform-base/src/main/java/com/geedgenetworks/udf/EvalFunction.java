package com.geedgenetworks.udf;

import com.geedgenetworks.expressions.EvalExecutor;
import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.pojo.ProjectionFunction;

import java.util.List;
import java.util.Map;

public class EvalFunction implements UdfFunction {

    private final String expr;

    private final String output;

    public EvalFunction(ProjectionFunction projectionFunction) {
        this.expr = (String) projectionFunction.getParameters().get("value_expression");
        List<String> outputField = projectionFunction.getOutput_fields();
        output = outputField.get(0);
    }

    @Override
    public void udffunction(Event event) {
        Map<String, Object> extractedFields = event.getExtractedFields();
        extractedFields.put(output, EvalExecutor.eval(expr, extractedFields));
    }
}
