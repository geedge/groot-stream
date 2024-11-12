package com.geedgenetworks.core.udf.udaf.HdrHistogram;

import com.alibaba.fastjson2.JSON;
import com.geedgenetworks.common.config.Accumulator;
import com.geedgenetworks.api.common.udf.UDFContext;
import org.HdrHistogram.Histogramer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HdrHistogramQuantiles  extends HdrHistogramBaseAggregate {
    double[] probabilities;

    @Override
    public void open(UDFContext c) {
        super.open(c);
        Map<String, Object> params = c.getParameters();
        Object ps = params.get("probabilities");
        if(ps == null){
            throw new IllegalArgumentException("probabilities param is requested");
        }
        List<Double> floats = JSON.parseArray(ps instanceof String ? ps.toString(): JSON.toJSONString(ps), Double.class);
        probabilities = new double[floats.size()];
        for (int i = 0; i < floats.size(); i++) {
            probabilities[i] = floats.get(i);
        }
    }

    @Override
    public Accumulator getResult(Accumulator acc) {
        Object agg = acc.getMetricsFields().get(outputField);
        if (agg == null) {
            return acc;
        }

        Histogramer his = ((Histogramer) agg);
        final List<Long> counts = new ArrayList<>(probabilities.length);
        for (int i = 0; i < probabilities.length; i++) {
            counts.add(his.getValueAtPercentile(probabilities[i] * 100));
        }
        acc.getMetricsFields().put(outputField, counts);
        return acc;
    }

    @Override
    public String functionName() {
        return "APPROX_QUANTILES_HDR";
    }

}
