package com.geedgenetworks.core.udf.udaf.HdrHistogram;

import com.geedgenetworks.common.config.Accumulator;
import com.geedgenetworks.sketch.util.StringUtils;
import com.geedgenetworks.api.common.udf.AggregateFunction;
import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.connector.event.Event;
import org.HdrHistogram.ArrayHistogram;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class HdrHistogramQuantileTest {
    AggregateFunction agg;
    Accumulator acc;
    Event event;

    @Test
    public void inputRegular() {
        double probability = 0.5;
        initData( "regular", 2, probability);
        long count = 100000;
        Map<String, Object> fields = event.getExtractedFields();
        for (int i = 1; i <= count; i++) {
            fields.put("ms", i);
            agg.add(event, acc);
        }

        long expect = (long) (count * probability);
        long rst = (long)agg.getResult(acc).getMetricsFields().get("ms_his");
        double error = Math.abs(rst - expect) / (double) expect;
        System.out.println(String.format("%s:%d,%d,%.4f", probability, expect ,  rst , error));
        assertTrue(error <= 0.05);
    }

    @Test
    public void inputSketch() {
        double probability = 0.5;
        initData( "sketch", 2, probability);
        long count = 100000;
        Map<String, Object> fields = event.getExtractedFields();

        ArrayHistogram his = new ArrayHistogram(2);
        for (int i = 1; i <= count; i++) {
            his.recordValue(i);
        }
        fields.put("ms", StringUtils.encodeBase64String(his.toBytes()));
        agg.add(event, acc);

        his = new ArrayHistogram(2);
        for (int i = 1; i <= count; i++) {
            his.recordValue(i);
        }
        fields.put("ms", his.toBytes());
        agg.add(event, acc);

        long expect = (long) (count * probability);
        long rst = (long)agg.getResult(acc).getMetricsFields().get("ms_his");
        double error = Math.abs(rst - expect) / (double) expect;
        System.out.println(String.format("%s:%d,%d,%.4f", probability, expect ,  rst , error));
        assertTrue(error <= 0.05);
    }

    private void initData(String input_type, int numberOfSignificantValueDigits, double probability){
        agg = new HdrHistogramQuantile();
        UDFContext c = new UDFContext();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("input_type", input_type);
        parameters.put("numberOfSignificantValueDigits", numberOfSignificantValueDigits);
        parameters.put("probability", probability);
        c.setParameters(parameters);
        c.setLookupFields(Collections.singletonList("ms"));
        c.setOutputFields(Collections.singletonList("ms_his"));

        agg.open(c);
        Map<String, Object> map = new HashMap<>();
        acc = new Accumulator();
        acc.setMetricsFields(map);
        agg.initAccumulator(acc);

        event = new Event();
        Map<String, Object> fields = new HashMap<>();
        event.setExtractedFields(fields);
    }
}