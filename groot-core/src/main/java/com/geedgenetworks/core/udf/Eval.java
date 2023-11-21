package com.geedgenetworks.core.udf;

import com.geedgenetworks.core.expressions.Calc;
import com.geedgenetworks.core.expressions.codegen.CalcCodeConvertor;
import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;

import java.util.List;
import java.util.Map;

public class Eval implements UDF {

    private String output;

    private Calc calc;

    @Override
    public void init(UDFContext udfContext) {
        String expr = (String) udfContext.getParameters().get("value_expression");
        List<String> outputField = udfContext.getOutput_fields();
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

    @Override
    public void close() {

    }
}
