package com.geedgenetworks.core.udf.udaf.hlld;

import com.geedgenetworks.common.Accumulator;
import com.geedgenetworks.sketch.hlld.Hll;

public class HlldApproxCountDistinct extends HlldBaseAggregate {

    @Override
    public Accumulator getResult(Accumulator acc) {
        Hll hll = getResultHll(acc);
        if (hll == null) {
            return acc;
        }

        acc.getMetricsFields().put(outputField, (long)hll.size());

        return acc;
    }

    @Override
    public String functionName() {
        return "APPROX_COUNT_DISTINCT_HLLD";
    }

}
