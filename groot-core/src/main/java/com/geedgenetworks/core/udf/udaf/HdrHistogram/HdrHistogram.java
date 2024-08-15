package com.geedgenetworks.core.udf.udaf.HdrHistogram;

import com.geedgenetworks.common.Accumulator;
import com.geedgenetworks.common.udf.UDFContext;
import com.geedgenetworks.sketch.util.StringUtils;
import org.HdrHistogram.Histogramer;

import java.util.Map;

public class HdrHistogram extends HdrHistogramBaseAggregate {
    boolean outputBase64;

    @Override
    public void open(UDFContext c) {
        super.open(c);
        Map<String, Object> params = c.getParameters();
        outputBase64 = "base64".equalsIgnoreCase(params.getOrDefault("output_format", "base64").toString());
    }

    @Override
    public Accumulator getResult(Accumulator acc) {
        Object agg = acc.getMetricsFields().get(outputField);
        if (agg == null) {
            return acc;
        }

        byte[] bytes = ((Histogramer) agg).toBytes();
        if (outputBase64) {
            acc.getMetricsFields().put(outputField, StringUtils.encodeBase64String(bytes));
        } else {
            acc.getMetricsFields().put(outputField, bytes);
        }

        return acc;
    }

    @Override
    public String functionName() {
        return "HDR_HISTOGRAM";
    }

}
