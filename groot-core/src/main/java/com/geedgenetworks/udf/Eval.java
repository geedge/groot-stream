package com.geedgenetworks.udf;

import com.geedgenetworks.expressions.Calc;
import com.geedgenetworks.expressions.codegen.CalcCodeConvertor;
import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.pojo.ProjectionFunction;

import java.util.List;
import java.util.Map;

public class Eval implements Udf {

    private String output;

    private Calc calc;

    @Override
    public void withParameters(ProjectionFunction projection_function) throws Exception {
        String expr = (String) projection_function.getParameters().get("value_expression");
        List<String> outputField = projection_function.getOutput_fields();
        output = outputField.get(0);
        calc = CalcCodeConvertor.convert(expr);
    }

    @Override
    public Event evaluate(Event event) {
        Map<String, Object> extractedFields = event.getExtractedFields();
        extractedFields.put(output, calc.eval(extractedFields));
        return event;
    }

    @Override
    public String functionName() {
        return "EVAL";
    }
}
