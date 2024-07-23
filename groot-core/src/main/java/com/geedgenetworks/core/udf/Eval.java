package com.geedgenetworks.core.udf;

import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.udf.ScalarFunction;
import com.geedgenetworks.common.udf.UDFContext;
import com.geedgenetworks.core.expressions.Calc;
import com.geedgenetworks.core.expressions.EvalExecutor;
import com.geedgenetworks.common.Event;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.util.List;
import java.util.Map;

public class Eval implements ScalarFunction {

    private String output;

    private Calc calc;

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {

        if(udfContext.getOutput_fields()==null || udfContext.getParameters() == null){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Missing required parameters");
        }
        if(!udfContext.getParameters().containsKey("value_expression")){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "parameters must containkey value_expression");
        }
        String expr = (String) udfContext.getParameters().get("value_expression");
        List<String> outputField = udfContext.getOutput_fields();
        output = outputField.get(0);
        calc = new EvalExecutor(expr);
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
