package com.geedgenetworks.core.udf.udaf.hlld;

import com.geedgenetworks.common.Accumulator;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.udf.AggregateFunction;
import com.geedgenetworks.common.udf.UDFContext;
import com.geedgenetworks.sketch.hlld.Hll;
import com.geedgenetworks.sketch.hlld.HllUnion;
import com.geedgenetworks.sketch.hlld.HllUtils;
import org.apache.commons.collections.CollectionUtils;

import java.util.Map;

public abstract class HlldBaseAggregate implements AggregateFunction {
    protected String inputField;
    protected String outputField;
    protected boolean inputSketch;
    protected int precision = 12;

    @Override
    public void open(UDFContext c) {
        inputField = c.getLookup_fields().get(0);
        if (CollectionUtils.isNotEmpty(c.getOutput_fields())) {
            outputField = c.getOutput_fields().get(0);
        } else {
            outputField = inputField;
        }
        Map<String, Object> params = c.getParameters();
        precision = Integer.parseInt(params.getOrDefault("precision", "12").toString());
        inputSketch = "sketch".equalsIgnoreCase(params.getOrDefault("input_type", "sketch").toString());
    }

    @Override
    public Accumulator initAccumulator(Accumulator acc) {
        return acc;
    }

    @Override
    public Accumulator add(Event event, Accumulator acc) {
        Object value = event.getExtractedFields().get(inputField);
        if (value == null) {
            return acc;
        }

        if (inputSketch) {
            updateHllUnion(acc, value);
        } else {
            updateHll(acc, value);
        }

        return acc;
    }

    protected Hll getResultHll(Accumulator acc){
        Object agg = acc.getMetricsFields().get(outputField);
        if (agg == null) {
            return null;
        }

        return inputSketch ? ((HllUnion) agg).getResult() : (Hll) agg;
    }

    protected void updateHll(Accumulator acc, Object value) {
        Map<String, Object> aggs = acc.getMetricsFields();
        Hll hll = (Hll) aggs.get(outputField);
        if (hll == null) {
            hll = new Hll(precision);
            aggs.put(outputField, hll);
        }

        if (value instanceof Integer || value instanceof Long) {
            hll.add(((Number) value).longValue());
        } else if (value instanceof Float || value instanceof Double) {
            hll.add(((Number) value).doubleValue());
        } else if (value instanceof String) {
            hll.add((String) value);
        } else if (value instanceof byte[]) {
            hll.add((byte[]) value);
        } else {
            throw new IllegalArgumentException("Unsupported type " + value.getClass());
        }
    }

    protected void updateHllUnion(Accumulator acc, Object value) {
        Map<String, Object> aggs = acc.getMetricsFields();
        HllUnion hllUnion = (HllUnion) aggs.get(outputField);
        if (hllUnion == null) {
            hllUnion = new HllUnion(precision);
            aggs.put(outputField, hllUnion);
        }

        Hll hll = HllUtils.deserializeHll(value);
        hllUnion.update(hll);
    }
    @Override
    public Accumulator merge(Accumulator a, Accumulator b) {
        return null;
    }
    @Override
    public void close() {}
}
