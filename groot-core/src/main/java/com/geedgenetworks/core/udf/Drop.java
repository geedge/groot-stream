package com.geedgenetworks.core.udf;

import com.geedgenetworks.common.config.CheckResult;
import com.geedgenetworks.common.config.CheckUDFContextUtil;
import com.geedgenetworks.common.config.UDFContextConfigOptions;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.udf.ScalarFunction;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.udf.UDFContext;
import org.apache.flink.api.common.functions.RuntimeContext;


public class Drop implements ScalarFunction {

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {
        checkConfig(udfContext);
    }

    @Override
    public Event evaluate(Event event) {
        event.setDropped(true);
        return event;
    }

    @Override
    public String functionName() {
        return "DROP";
    }

    @Override
    public void checkConfig(UDFContext udfContext) {
        CheckResult result = CheckUDFContextUtil.checkAllExists(udfContext,
                UDFContextConfigOptions.FILTER.key());

        if (!result.isSuccess()) {
            throw new GrootStreamRuntimeException(CommonErrorCode.CONFIG_VALIDATION_FAILED, result.getMsg());
        }

    }

    @Override
    public void close() {

    }

}
