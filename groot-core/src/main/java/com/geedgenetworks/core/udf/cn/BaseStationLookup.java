package com.geedgenetworks.core.udf.cn;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.udf.UDFContext;
import com.geedgenetworks.core.udf.knowlegdebase.KnowledgeBaseUpdateJob;
import com.geedgenetworks.core.udf.knowlegdebase.handler.BaseStationKnowledgeBaseHandler;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.util.List;
import java.util.Map;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/8/19 15:21
 */
public class BaseStationLookup extends AbstractKnowledgeScalarFunction {

    private String cellIdFieldName;

    private String longitudeFieldName;

    private String latitudeFieldName;

    private BaseStationKnowledgeBaseHandler knowledgeBaseHandler;

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {
        super.open(runtimeContext, udfContext);
        if (udfContext.getLookupFields() == null || udfContext.getOutputFields() == null || udfContext.getParameters() == null) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Missing required parameters");
        }

        int lookupFieldsSize = 1;
        if (udfContext.getLookupFields().size() != lookupFieldsSize) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "lookup_fields must contain field: cell_id");
        }

        int outputFieldsSize = 2;
        if (udfContext.getOutputFields().size() != outputFieldsSize) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "output_fields must contain two fields: longitude and latitude");
        }

        List<String> lookupFields = udfContext.getLookupFields();
        cellIdFieldName = lookupFields.get(0);
        List<String> outputFields = udfContext.getOutputFields();
        longitudeFieldName = outputFields.get(0);
        latitudeFieldName = outputFields.get(1);
    }

    @Override
    public Event evaluate(Event event) {
        Map<String, Object> extractedFields = event.getExtractedFields();
        if (extractedFields == null || extractedFields.get(cellIdFieldName) == null) {
            return event;
        }
        BaseStationKnowledgeBaseHandler.BaseStationLocation lookup = knowledgeBaseHandler.lookup(extractedFields.get(cellIdFieldName).toString());
        if (lookup != null) {
            extractedFields.put(longitudeFieldName, lookup.getLongitude());
            extractedFields.put(latitudeFieldName, lookup.getLatitude());
        }
        return event;
    }

    @Override
    public String functionName() {
        return "BASE_STATION_LOOKUP";
    }

    @Override
    public void close() {
    }

    @Override
    protected void registerKnowledges() {
        knowledgeBaseHandler = BaseStationKnowledgeBaseHandler.getInstance();
        KnowledgeBaseUpdateJob.registerKnowledgeBase(knowledgeBaseHandler, knowledgeBaseConfigs.get(0));
    }
}
