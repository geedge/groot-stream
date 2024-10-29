package com.geedgenetworks.core.udf.uuid;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.RandomBasedGenerator;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.udf.ScalarFunction;
import com.geedgenetworks.common.udf.UDFContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;

@Slf4j
public class UUID implements ScalarFunction {
    private String outputFieldName;
    private RandomBasedGenerator randomBasedGenerator;
    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {

        if(udfContext.getOutputFields()==null ){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Missing required parameters");
        }
        if(udfContext.getOutputFields().size() != 1){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function output fields only support 1 value");
        }
        this.outputFieldName = udfContext.getOutputFields().get(0);
        this.randomBasedGenerator = Generators.randomBasedGenerator();
    }

    @Override
    public Event evaluate(Event event) {

        event.getExtractedFields()
                .put(outputFieldName, randomBasedGenerator.generate().toString());
        return event;
    }

    @Override
    public String functionName() {
        return "UUID";
    }

    @Override
    public void close() {
    }


}
