package com.geedgenetworks.core.udf.udaf.hlld;

import com.geedgenetworks.common.Accumulator;
import com.geedgenetworks.common.udf.UDFContext;
import com.geedgenetworks.sketch.hlld.Hll;
import com.geedgenetworks.sketch.util.StringUtils;

import java.util.Map;

public class Hlld extends HlldBaseAggregate {
    boolean outputBase64;

    @Override
    public void open(UDFContext c) {
        super.open(c);
        Map<String, Object> params = c.getParameters();
        outputBase64 = "base64".equalsIgnoreCase(params.getOrDefault("output_format", "base64").toString());
    }

    @Override
    public Accumulator getResult(Accumulator acc) {
        Hll hll = getResultHll(acc);
        if (hll == null) {
            return acc;
        }

        if (outputBase64) {
            acc.getMetricsFields().put(outputField, StringUtils.encodeBase64String(hll.toBytes()));
        } else {
            acc.getMetricsFields().put(outputField, hll.toBytes());
        }

        return acc;
    }

    @Override
    public String functionName() {
        return "HLLD";
    }
}
