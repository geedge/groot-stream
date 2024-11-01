package com.geedgenetworks.common.udf;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.config.CheckUDFContextUtil;
import com.geedgenetworks.common.config.UDFContextConfigOptions;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import org.apache.flink.api.common.functions.RuntimeContext;
import java.io.Serializable;

public interface ScalarFunction extends Serializable {

    void open(RuntimeContext runtimeContext, UDFContext udfContext);

    Event evaluate(Event event);

    String functionName();

    void close();

    default void checkConfig(UDFContext udfContext) {
        if (udfContext == null) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "UDFContext cannot be null");
        }

        if (!CheckUDFContextUtil.checkAtLeastOneExists(udfContext, UDFContextConfigOptions.LOOKUP_FIELDS.key(), UDFContextConfigOptions.OUTPUT_FIELDS.key(), UDFContextConfigOptions.FILTER.key()).isSuccess()) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "At least one of the config should be specified.");
        }

    }

}
