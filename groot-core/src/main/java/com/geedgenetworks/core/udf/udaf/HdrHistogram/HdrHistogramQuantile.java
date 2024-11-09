package com.geedgenetworks.core.udf.udaf.HdrHistogram;

import com.geedgenetworks.common.config.Accumulator;
import com.geedgenetworks.spi.common.udf.UDFContext;
import org.HdrHistogram.Histogramer;

import java.util.Map;

public class HdrHistogramQuantile extends HdrHistogramBaseAggregate {
    Double probability;

    @Override
    public void open(UDFContext c) {
        super.open(c);
        Map<String, Object> params = c.getParameters();
        probability = Double.parseDouble(params.getOrDefault("probability", "0.5").toString());
    }

    @Override
    public Accumulator getResult(Accumulator acc) {
        Object agg = acc.getMetricsFields().get(outputField);
        if (agg == null) {
            return acc;
        }

        long percentile = ((Histogramer) agg).getValueAtPercentile(probability * 100);
        acc.getMetricsFields().put(outputField, percentile);
        return acc;
    }

    @Override
    public String functionName() {
        return "APPROX_QUANTILE_HDR";
    }

}
