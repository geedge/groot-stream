package com.geedgenetworks.core.udf.cn;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.udf.ScalarFunction;
import com.geedgenetworks.common.udf.UDFContext;
import com.uber.h3core.H3Core;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/8/19 14:50
 */
public class H3CellLookup implements ScalarFunction {

    private String longitudeFieldName;

    private String latitudeFieldName;

    private String outputFieldName;

    private int res;

    private H3Core h3;


    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {
        if (udfContext.getLookup_fields() == null || udfContext.getOutput_fields() == null || udfContext.getParameters() == null) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Missing required parameters");
        }

        int lookupFieldsSize = 2;
        if (udfContext.getLookup_fields().size() != lookupFieldsSize) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "lookup_fields must contain two fields: longitude and latitude");
        }

        String resolution = "resolution";
        int maxResolution = 15;
        if (!udfContext.getParameters().containsKey(resolution) || !(udfContext.getParameters().get(resolution) instanceof Integer) || (int) udfContext.getParameters().get(resolution) < 0 || (int) udfContext.getParameters().get(resolution) > maxResolution) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "parameters must contain key resolution and the value is between 0 and 15");
        }

        List<String> lookupFields = udfContext.getLookup_fields();
        longitudeFieldName = lookupFields.get(0);
        latitudeFieldName = lookupFields.get(1);
        outputFieldName = udfContext.getOutput_fields().get(0);
        res = (int) udfContext.getParameters().get("resolution");
        try {
            h3 = H3Core.newInstance();
        } catch (IOException io) {
            throw new GrootStreamRuntimeException(CommonErrorCode.FILE_OPERATION_ERROR, "Failed to create H3Core instance");
        }
    }

    @Override
    public Event evaluate(Event event) {
        Map<String, Object> extractedFields = event.getExtractedFields();
        if (extractedFields == null || extractedFields.get(longitudeFieldName) == null || extractedFields.get(latitudeFieldName) == null) {
            return event;
        }
        if (!(extractedFields.get(longitudeFieldName) instanceof Double) || !(extractedFields.get(latitudeFieldName) instanceof Double)) {
            return event;
        }
        String cellAddress = h3.latLngToCellAddress((double) extractedFields.get(latitudeFieldName), (double) extractedFields.get(longitudeFieldName), res);
        extractedFields.put(outputFieldName, cellAddress);
        return event;
    }

    @Override
    public String functionName() {
        return "H3_CELL_LOOKUP";
    }

    @Override
    public void close() {
    }
}
