package com.geedgenetworks.core.udf;

import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.core.utils.SnowflakeIdUtils;
import com.geedgenetworks.api.common.udf.ScalarFunction;
import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.event.Event;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.io.Serializable;

public class SnowflakeId implements Serializable, ScalarFunction {

    private String outputFieldName;
    private SnowflakeIdUtils snowflakeIdUtils;
    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {

        if(udfContext.getOutputFields()==null  || udfContext.getParameters() == null ){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Missing required parameters");
        }
        if(udfContext.getOutputFields().size() != 1){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function output fields only support 1 value");
        }
        String data_center_id_num = udfContext.getParameters().getOrDefault("data_center_id_num","0").toString();//转为数字
        snowflakeIdUtils = new SnowflakeIdUtils(Integer.parseInt(data_center_id_num),runtimeContext.getIndexOfThisSubtask());
        this.outputFieldName = udfContext.getOutputFields().get(0);
    }

    @Override
    public Event evaluate(Event event) {

        event.getExtractedFields()
                .put(outputFieldName, snowflakeIdUtils.nextId());
        return event;
    }

    @Override
    public String functionName() {
        return "SNOWFLAKE_ID";
    }

    @Override
    public void close() {

    }
}
