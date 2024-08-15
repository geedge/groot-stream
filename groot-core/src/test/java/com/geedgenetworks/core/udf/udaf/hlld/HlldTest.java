package com.geedgenetworks.core.udf.udaf.hlld;

import com.geedgenetworks.common.Accumulator;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.udf.AggregateFunction;
import com.geedgenetworks.common.udf.UDFContext;
import com.geedgenetworks.sketch.hlld.Hll;
import com.geedgenetworks.sketch.util.StringUtils;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class HlldTest {
    AggregateFunction agg;
    Accumulator acc;
    Event event;

    @Test
    public void inputRegular() {
        initData(14, "regular", "base64");
        long count = 100000;
        Map<String, Object> fields = event.getExtractedFields();
        for (int i = 0; i < count; i++) {
            fields.put("ip", i);
            agg.add(event, acc);
        }

        String hllStr = (String)agg.getResult(acc).getMetricsFields().get("ip_cnt");
        long rst = (long) Hll.fromBytes(StringUtils.decodeBase64(hllStr.getBytes(StandardCharsets.UTF_8))).size();
        double error = Math.abs(rst - count) / (double) count;
        System.out.println(String.format("%d,%d,%.4f", count ,  rst , error));
        assertTrue(error <= 0.05);
    }

    @Test
    public void inputSketch() {
        initData(14, "sketch", "binary");
        long count = 150000;
        Map<String, Object> fields = event.getExtractedFields();
        for (int i = 0; i < 100000; i++) {
            Hll hll = new Hll(12);
            hll.add(i);
            fields.put("ip", StringUtils.encodeBase64String(hll.toBytes()));
            agg.add(event, acc);
        }
        for (int i = 50000; i < 150000; i++) {
            Hll hll = new Hll(13);
            hll.add(i);
            fields.put("ip", hll.toBytes());
            agg.add(event, acc);
        }

        byte[] hllBytes = (byte[])agg.getResult(acc).getMetricsFields().get("ip_cnt");
        long rst = (long) Hll.fromBytes(hllBytes).size();
        double error = Math.abs(rst - count) / (double) count;
        System.out.println(String.format("%d,%d,%.4f", count ,  rst , error));
        assertTrue(error <= 0.05);
    }

    private void initData(int precision, String input_type, String output_format){
        agg = new Hlld();
        UDFContext c = new UDFContext();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("precision", precision);
        parameters.put("input_type", input_type);
        parameters.put("output_format", output_format);
        c.setParameters(parameters);
        c.setLookup_fields(Collections.singletonList("ip"));
        c.setOutput_fields(Collections.singletonList("ip_cnt"));

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
