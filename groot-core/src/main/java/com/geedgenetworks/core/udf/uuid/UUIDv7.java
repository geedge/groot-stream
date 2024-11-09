package com.geedgenetworks.core.udf.uuid;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedEpochGenerator;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.spi.common.udf.ScalarFunction;
import com.geedgenetworks.spi.common.udf.UDFContext;
import com.geedgenetworks.spi.table.event.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;

@Slf4j
public class UUIDv7 implements ScalarFunction {
    private String outputFieldName;
    private TimeBasedEpochGenerator timeBasedEpochRandomGenerator;

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {

        if(udfContext.getOutputFields()==null ){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Missing required parameters");
        }
        if(udfContext.getOutputFields().size() != 1){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function output fields only support 1 value");
        }
        this.outputFieldName = udfContext.getOutputFields().get(0);
        this.timeBasedEpochRandomGenerator = Generators.timeBasedEpochGenerator();

    }

    @Override
    public Event evaluate(Event event) {
        event.getExtractedFields()
                .put(outputFieldName, timeBasedEpochRandomGenerator.generate().toString());
        return event;
    }

    @Override
    public String functionName() {
        return "UUIDv7";
    }

    @Override
    public void close() {
    }
}
