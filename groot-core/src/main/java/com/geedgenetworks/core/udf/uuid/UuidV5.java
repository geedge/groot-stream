package com.geedgenetworks.core.udf.uuid;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.NameBasedGenerator;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.udf.ScalarFunction;
import com.geedgenetworks.common.udf.UDFContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.util.List;

@Slf4j
public class UuidV5 implements ScalarFunction {
    private List<String> lookupFieldNames;
    private String outputFieldName;
    private NameBasedGenerator nameBasedGenerator;
    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {

        if(udfContext.getOutput_fields()==null || udfContext.getParameters()==null || udfContext.getLookup_fields()==null){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Missing required parameters");
        }
        if(udfContext.getOutput_fields().size() != 1){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function output fields only support 1 value");
        }
        if(!udfContext.getParameters().containsKey("namespace") ){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "parameters must containkey namespace");
        }
        this.outputFieldName = udfContext.getOutput_fields().get(0);
        this.lookupFieldNames = udfContext.getOutput_fields();
        this.nameBasedGenerator = Generators.nameBasedGenerator(NameSpace.getUuidByName(udfContext.getParameters().get("namespace").toString()));
    }

    @Override
    public Event evaluate(Event event) {
        StringBuilder sb = new StringBuilder();
        for(String lookupField:lookupFieldNames){
            sb.append(event.getExtractedFields().getOrDefault(lookupField,""));
        }
        event.getExtractedFields()
                .put(outputFieldName, nameBasedGenerator.generate(sb.toString()));
        return event;
    }

    @Override
    public String functionName() {
        return "UUIDv5";
    }

    @Override
    public void close() {

    }
}
