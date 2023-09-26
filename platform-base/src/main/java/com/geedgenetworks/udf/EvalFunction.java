package com.geedgenetworks.udf;

import com.geedgenetworks.expressions.Calc;
import com.geedgenetworks.expressions.codegen.CalcCodeConvertor;
import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.pojo.ProjectionFunction;

import java.util.List;
import java.util.Map;

public class EvalFunction implements UdfFunction {

    private final String output;

    private final Calc calc;

    public EvalFunction(ProjectionFunction projectionFunction) throws Exception {
        String expr = (String) projectionFunction.getParameters().get("value_expression");
        List<String> outputField = projectionFunction.getOutput_fields();
        output = outputField.get(0);
        calc = CalcCodeConvertor.convert(expr);
    }

    @Override
    public void udffunction(Event event) {
        Map<String, Object> extractedFields = event.getExtractedFields();
        extractedFields.put(output, calc.eval(extractedFields));
    }
}
