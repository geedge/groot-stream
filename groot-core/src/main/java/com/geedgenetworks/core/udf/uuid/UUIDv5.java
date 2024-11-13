package com.geedgenetworks.core.udf.uuid;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.NameBasedGenerator;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.api.common.udf.ScalarFunction;
import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.event.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.util.List;

@Slf4j
public class UUIDv5 implements ScalarFunction {
    private List<String> lookupFieldNames;
    private String outputFieldName;
    private NameBasedGenerator nameBasedGenerator;
    private static final String NAMESPACE_KEY = "namespace";
    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {

        if(udfContext.getOutputFields() == null || udfContext.getParameters() == null || udfContext.getLookupFields() == null){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Missing required parameters");
        }
        if(udfContext.getOutputFields().size() != 1){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function output fields only support 1 value");
        }
        if(!udfContext.getParameters().containsKey(NAMESPACE_KEY) ){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Parameters must contain key: " + NAMESPACE_KEY);
        }

        this.outputFieldName = udfContext.getOutputFields().get(0);
        this.lookupFieldNames = udfContext.getLookupFields();
        String namespace = udfContext.getParameters().get(NAMESPACE_KEY).toString();
        this.nameBasedGenerator = Generators.nameBasedGenerator(UUIDNameSpace.getUUID(namespace));
    }

    @Override
    public Event evaluate(Event event) {

        String concatenatedFields = String.join("_",
                lookupFieldNames.stream()
                        .map(field -> event.getExtractedFields().getOrDefault(field, ""))
                        .toArray(String[]::new)
        );

        // Generate the UUID based on concatenated fields
        String generatedUUID = nameBasedGenerator.generate(concatenatedFields).toString();

        // Set the generated UUID in the output field
        event.getExtractedFields().put(outputFieldName, generatedUUID);
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
