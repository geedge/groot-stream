package com.geedgenetworks.core.udf;

import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;
import com.geedgenetworks.core.utils.SnowflakeIdUtils;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.io.Serializable;

public class SnowflakeId implements Serializable, UDF {

    private UDFContext udfContext;

    private SnowflakeIdUtils snowflakeIdUtils;
    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {
        String data_center_id_num = udfContext.getParameters().getOrDefault("data_center_id_num","0").toString();//转为数字
        snowflakeIdUtils = new SnowflakeIdUtils(Integer.parseInt(data_center_id_num),runtimeContext.getIndexOfThisSubtask());
        this.udfContext = udfContext;
    }

    @Override
    public Event evaluate(Event event) {

        event.getExtractedFields()
                .put(udfContext.getOutput_fields().get(0), snowflakeIdUtils.nextId());
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
