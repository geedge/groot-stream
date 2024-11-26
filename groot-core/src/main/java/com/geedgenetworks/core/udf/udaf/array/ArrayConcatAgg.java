package com.geedgenetworks.core.udf.udaf.array;


import com.geedgenetworks.api.common.udf.AggregateFunction;
import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.event.Event;
import com.geedgenetworks.common.config.Accumulator;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;

import java.util.*;

/**
 * Aggregates elements within a group and returns a collection based on mode (list or set).
 */
public class ArrayConcatAgg implements AggregateFunction {

    private String lookupField;
    private String outputField;
    private String mode;
    private long maxSize;

    @Override
    public void open(UDFContext udfContext) {
        // Validate input parameters
        if (udfContext.getLookupFields() == null || udfContext.getLookupFields().isEmpty() || udfContext.getParameters() == null) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Missing required lookup field parameter");
        }
        this.lookupField = udfContext.getLookupFields().get(0);
        this.outputField = (udfContext.getOutputFields() == null || udfContext.getOutputFields().isEmpty())
                ? lookupField : udfContext.getOutputFields().get(0);
        this.mode = (String) udfContext.getParameters().getOrDefault("mode", "all");
        this.maxSize = Long.parseLong(udfContext.getParameters().getOrDefault("max_size", 1000000L).toString());
    }

    @Override
    public Accumulator initAccumulator(Accumulator acc) {
        acc.getMetricsFields().put(outputField, createLimitedCollection());
        return acc;
    }

    @Override
    public Accumulator add(Event event, Accumulator acc) {
        Object valueObj = event.getExtractedFields().get(lookupField);
        if (valueObj instanceof List<?>) {
            Collection<Object> collection = getOrInitCollection(acc);
            collection.addAll((List<?>) valueObj);
            acc.getMetricsFields().put(outputField, collection);
        }
        return acc;
    }

    @Override
    public Accumulator merge(Accumulator firstAcc, Accumulator secondAcc) {
        Collection<Object> firstCollection = getOrInitCollection(firstAcc);
        Collection<Object> secondCollection = getOrInitCollection(secondAcc);

        if (firstCollection != null && secondCollection != null) {
            firstCollection.addAll(secondCollection);
        } else if (firstCollection == null) {
            firstAcc.getMetricsFields().put(outputField, secondCollection);
        }

        return firstAcc;
    }

    @Override
    public Accumulator getResult(Accumulator acc) {
        Collection<Object> collection = getOrInitCollection(acc);
        // Ensure the result does not exceed maxSize
        if (collection instanceof List) {
            // Limit the size of List
            collection = limitListSize((List<Object>) collection);
        } else if (collection instanceof Set) {
            // Limit the size of Set (converted to a list for limiting)
            collection = limitSetSize((Set<Object>) collection);
        }
        acc.getMetricsFields().put(outputField, collection);
        return acc;
    }

    @Override
    public String functionName() {
        return "ARRAY_CONCAT_AGG";
    }

    @SuppressWarnings("unchecked")
    private Collection<Object> getOrInitCollection(Accumulator acc) {
        return (Collection<Object>) acc.getMetricsFields().computeIfAbsent(outputField, k -> createLimitedCollection());
    }

    private Collection<Object> createLimitedCollection() {
        return "all".equals(mode) ? new LinkedList<>() : new HashSet<>();
    }
    private List<Object> limitListSize(List<Object> list) {
        return list.size() > maxSize ? list.subList(0, (int) maxSize) : list;
    }

    private Set<Object> limitSetSize(Set<Object> set) {
        List<Object> list = new ArrayList<>(set);
        list = list.size() > maxSize ? list.subList(0, (int) maxSize) : list;
        return new HashSet<>(list);
    }
}


