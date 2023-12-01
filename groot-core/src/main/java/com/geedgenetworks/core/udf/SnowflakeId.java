package com.geedgenetworks.core.udf;

import com.geedgenetworks.common.config.CommonConfig;
import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;
import com.geedgenetworks.core.udf.UDF;
import com.geedgenetworks.core.utils.SnowflakeIdUtils;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.io.Serializable;

public class SnowflakeId implements Serializable, UDF {

    private UDFContext udfContext;

    private SnowflakeIdUtils snowflakeIdUtils;
    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {
        snowflakeIdUtils = new SnowflakeIdUtils(CommonConfig.DATA_CENTER_ID_NUM,runtimeContext.getIndexOfThisSubtask());
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
