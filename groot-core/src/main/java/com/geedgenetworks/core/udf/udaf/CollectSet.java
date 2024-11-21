package com.geedgenetworks.core.udf.udaf;


import com.geedgenetworks.common.config.Accumulator;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.api.common.udf.AggregateFunction;
import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.event.Event;


import java.util.*;

/**
 * Collects elements within a group and returns the list of aggregated objects
 */
public class CollectSet implements AggregateFunction {

    private String lookupField;
    private String outputField;
    private String collectType;

    @Override
    public void open(UDFContext udfContext) {
        // 校验输入字段
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
        acc.getMetricsFields().put(outputField, new HashSet<>());
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
                Set<Object> aggregateSet = getOrInitSet(acc);
                aggregateSet.addAll(valueList);
            }
        } else {
            getOrInitSet(acc).add(valueObj);
        }

        return acc;
    }

    @Override
    public Accumulator merge(Accumulator firstAcc, Accumulator secondAcc) {
        Object firstValueObj = firstAcc.getMetricsFields().get(outputField);
        Object secondValueObj = secondAcc.getMetricsFields().get(outputField);
        mergeSets(firstAcc, firstValueObj, secondValueObj);
        return firstAcc;
    }

    @Override
    public Accumulator getResult(Accumulator acc) {
        return acc;
    }

    @Override
    public String functionName() {
        return "COLLECT_SET";
    }

    private Set<Object> getOrInitSet(Accumulator acc) {
        return (Set<Object>) acc.getMetricsFields()
                .computeIfAbsent(outputField, k -> new HashSet<>());
    }

    @SuppressWarnings("unchecked")
    private void mergeSets(Accumulator acc, Object firstValueObj, Object secondValueObj) {
        if (firstValueObj instanceof Set<?> && secondValueObj instanceof Set<?>) {
            ((Set<Object>) firstValueObj).addAll((Set<Object>) secondValueObj);
        } else if (firstValueObj == null && secondValueObj instanceof Set<?>) {
            acc.getMetricsFields().put(outputField, secondValueObj);
        }
    }


}
