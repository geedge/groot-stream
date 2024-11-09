package com.geedgenetworks.core.udf.udaf.hlld;


import com.geedgenetworks.common.config.Accumulator;
import com.geedgenetworks.sketch.hlld.Hll;
import com.geedgenetworks.sketch.util.StringUtils;
import com.geedgenetworks.spi.common.udf.AggregateFunction;
import com.geedgenetworks.spi.common.udf.UDFContext;
import com.geedgenetworks.spi.table.event.Event;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;


public class HlldApproxCountDistinctTest {
    AggregateFunction agg;
    Accumulator acc;
    Event event;


    @Test
    public void inputRegular() {
        initData(14, "regular");
        long count = 100000;
        Map<String, Object> fields = event.getExtractedFields();
        for (int i = 0; i < count; i++) {
            fields.put("ip", i);
            agg.add(event, acc);
        }

        long rst = (long)agg.getResult(acc).getMetricsFields().get("ip_cnt");
        double error = Math.abs(rst - count) / (double) count;
        System.out.println(String.format("%d,%d,%.4f", count ,  rst , error));
        assertTrue(error <= 0.05);
    }

    @Test
    public void inputSketch() {
        initData(14, "sketch");
        long count = 150000;
        Map<String, Object> fields = event.getExtractedFields();

        Hll hll = new Hll(12);
        for (int i = 0; i < 100000; i++) {
            hll.add(i);
        }
        fields.put("ip", StringUtils.encodeBase64String(hll.toBytes()));
        agg.add(event, acc);

        hll = new Hll(13);
        for (int i = 50000; i < 150000; i++) {
            hll.add(i);
        }
        fields.put("ip", hll.toBytes());
        agg.add(event, acc);

        long rst = (long)agg.getResult(acc).getMetricsFields().get("ip_cnt");
        double error = Math.abs(rst - count) / (double) count;
        System.out.println(String.format("%d,%d,%.4f", count ,  rst , error));
        assertTrue(error <= 0.05);
    }

    private void initData(int precision, String input_type){
        agg = new HlldApproxCountDistinct();
        UDFContext c = new UDFContext();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("precision", precision);
        parameters.put("input_type", input_type);
        c.setParameters(parameters);
        c.setLookupFields(Collections.singletonList("ip"));
        c.setOutputFields(Collections.singletonList("ip_cnt"));

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