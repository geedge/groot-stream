package com.geedgenetworks.core.udf.uuid;

import com.fasterxml.uuid.Generators;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.udf.ScalarFunction;
import com.geedgenetworks.common.udf.UDFContext;
import com.geedgenetworks.core.utils.SnowflakeIdUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.util.List;
import java.util.UUID;

import static com.geedgenetworks.utils.FormatUtils.getTopPrivateDomain;

@Slf4j
public class Uuid implements ScalarFunction {
    private String outputFieldName;
    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {

        if(udfContext.getOutput_fields()==null ){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Missing required parameters");
        }
        if(udfContext.getOutput_fields().size() != 1){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function output fields only support 1 value");
        }
        this.outputFieldName = udfContext.getOutput_fields().get(0);
    }

    @Override
    public Event evaluate(Event event) {

        event.getExtractedFields()
                .put(outputFieldName, Generators.randomBasedGenerator().generate());
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
