package com.geedgenetworks.core.udf.udaf;

import com.geedgenetworks.api.common.udf.AggregateFunction;
import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.event.Event;
import com.geedgenetworks.common.config.Accumulator;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;

import java.util.*;

/**
 * Collects elements within a group and returns the list of aggregated objects
 */
public class CollectList implements AggregateFunction {

    private String lookupField;
    private String outputField;
    private String collectType;

    @Override
    public void open(UDFContext udfContext) {
        // Validate input fields
        if (udfContext.getLookupFields() == null || udfContext.getLookupFields().isEmpty()) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Missing required lookup field parameter");
        }
        this.lookupField = udfContext.getLookupFields().get(0);
        this.outputField = udfContext.getOutputFields() == null || udfContext.getOutputFields().isEmpty()
                ? lookupField
                : udfContext.getOutputFields().get(0);
        this.collectType = udfContext.getParameters() == null
                ? "object"
                : udfContext.getParameters().getOrDefault("collect_type", "object").toString();
    }

    @Override
    public Accumulator initAccumulator(Accumulator acc) {
        acc.getMetricsFields().put(outputField, new ArrayList<>());
        return acc;
    }

    @Override
    public Accumulator add(Event event, Accumulator acc) {
        Object valueObj = event.getExtractedFields().get(lookupField);
        if (valueObj == null) {
            return acc;
        }
        if (collectType.equals("array")) {
            if (valueObj instanceof List<?>) {
                List<?> valueList = (List<?>) valueObj;
                List<Object> aggregateList = getOrInitList(acc);
                aggregateList.addAll(valueList);
            }
        } else {
            getOrInitList(acc).add(valueObj);
        }

        return acc;
    }

    @Override
    public Accumulator merge(Accumulator firstAcc, Accumulator secondAcc) {
        Object firstValueObj = firstAcc.getMetricsFields().get(outputField);
        Object secondValueObj = secondAcc.getMetricsFields().get(outputField);
        mergeLists(firstAcc, firstValueObj, secondValueObj);
        return firstAcc;
    }

    @Override
    public Accumulator getResult(Accumulator acc) {
        return acc;
    }

    @Override
    public String functionName() {
        return "COLLECT_LIST";
    }

    private List<Object> getOrInitList(Accumulator acc) {
        return (List<Object>) acc.getMetricsFields()
                .computeIfAbsent(outputField, k -> new ArrayList<>());
    }


    @SuppressWarnings("unchecked")
    private void mergeLists(Accumulator acc, Object firstValueObj, Object secondValueObj) {
        if (firstValueObj instanceof List<?> && secondValueObj instanceof List<?>) {
            ((List<Object>) firstValueObj).addAll((List<Object>) secondValueObj);
        } else if (firstValueObj == null && secondValueObj instanceof List<?>) {
            acc.getMetricsFields().put(outputField, secondValueObj);
        }
    }



}
