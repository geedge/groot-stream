package com.geedgenetworks.core.udf;

import com.geedgenetworks.common.udf.UDF;
import com.geedgenetworks.common.udf.UDFContext;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.core.utils.SnowflakeIdUtils;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.io.Serializable;

public class SnowflakeId implements Serializable, UDF {

    private String outputFieldName;
    private SnowflakeIdUtils snowflakeIdUtils;
    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {

        if(udfContext.getOutput_fields()==null  || udfContext.getParameters() == null ){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Missing required parameters");
        }
        if(udfContext.getOutput_fields().size() != 1){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function output fields only support 1 value");
        }
        String data_center_id_num = udfContext.getParameters().getOrDefault("data_center_id_num","0").toString();//转为数字
        snowflakeIdUtils = new SnowflakeIdUtils(Integer.parseInt(data_center_id_num),runtimeContext.getIndexOfThisSubtask());
        this.outputFieldName = udfContext.getOutput_fields().get(0);
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
