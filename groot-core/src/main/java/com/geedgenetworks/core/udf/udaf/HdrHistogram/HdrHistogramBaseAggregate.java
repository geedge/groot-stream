package com.geedgenetworks.core.udf.udaf.HdrHistogram;

import com.geedgenetworks.common.Accumulator;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.udf.AggregateFunction;
import com.geedgenetworks.common.udf.UDFContext;
import com.geedgenetworks.sketch.util.StringUtils;
import org.HdrHistogram.ArrayHistogram;
import org.HdrHistogram.DirectMapHistogram;
import org.HdrHistogram.Histogramer;
import org.apache.commons.collections.CollectionUtils;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public abstract class HdrHistogramBaseAggregate implements AggregateFunction {
    protected String inputField;
    protected String outputField;
    protected boolean inputSketch;
    protected long lowestDiscernibleValue;
    protected long highestTrackableValue;
    protected int numberOfSignificantValueDigits;
    protected boolean autoResize;

    @Override
    public void open(UDFContext c) {
        inputField = c.getLookup_fields().get(0);
        if (CollectionUtils.isNotEmpty(c.getOutput_fields())) {
            outputField = c.getOutput_fields().get(0);
        } else {
            outputField = inputField;
        }
        Map<String, Object> params = c.getParameters();
        lowestDiscernibleValue = Long.parseLong(params.getOrDefault("lowestDiscernibleValue", "1").toString());
        highestTrackableValue = Long.parseLong(params.getOrDefault("highestTrackableValue", "2").toString());
        numberOfSignificantValueDigits = Integer.parseInt(params.getOrDefault("numberOfSignificantValueDigits", "1").toString());
        autoResize = Boolean.valueOf(params.getOrDefault("autoResize", "true").toString());
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
            updateHdrMerge(acc, value);
        } else {
            updateHdr(acc, value);
        }

        return acc;
    }

    protected void updateHdr(Accumulator acc, Object value) {
        Map<String, Object> aggs = acc.getMetricsFields();
        ArrayHistogram his = (ArrayHistogram) aggs.get(outputField);
        if (his == null) {
            his = new ArrayHistogram(lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits);
            his.setAutoResize(autoResize);
            aggs.put(outputField, his);
        }

        his.recordValue(((Number) value).longValue());
    }


    protected void updateHdrMerge(Accumulator acc, Object value) {
        Map<String, Object> aggs = acc.getMetricsFields();
        ArrayHistogram his = (ArrayHistogram) aggs.get(outputField);
        if (his == null) {
            his = new ArrayHistogram(lowestDiscernibleValue, highestTrackableValue, numberOfSignificantValueDigits);
            his.setAutoResize(autoResize);
            aggs.put(outputField, his);
        }

        Histogramer h;
        if (value instanceof String) {
            byte[] bytes = StringUtils.decodeBase64(((String) value).getBytes(StandardCharsets.UTF_8));
            h = DirectMapHistogram.wrapBytes(bytes);
        } else if (value instanceof byte[]) {
            h = DirectMapHistogram.wrapBytes((byte[]) value);
        } else if (value instanceof Histogramer) {
            h = (Histogramer) value;
        } else {
            throw new IllegalArgumentException("Unsupported type " + value.getClass());
        }

        his.merge(h);
    }
    @Override
    public Accumulator merge(Accumulator a, Accumulator b) {
        return null;
    }
    @Override
    public void close() {}
}
