package com.geedgenetworks.core.udf.udaf.HdrHistogram;

import com.geedgenetworks.common.Accumulator;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.udf.AggregateFunction;
import com.geedgenetworks.common.udf.UDFContext;
import com.geedgenetworks.sketch.util.StringUtils;
import org.HdrHistogram.ArrayHistogram;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class HdrHistogramTest {
    AggregateFunction agg;
    Accumulator acc;
    Event event;

    @Test
    public void inputRegular() {
        double[] probabilities = new double[]{0, 0.1, 0.25, 0.5, 0.75, 1};
        initData( "regular", 2, "base64");
        long count = 100000;
        Map<String, Object> fields = event.getExtractedFields();
        for (int i = 1; i <= count; i++) {
            fields.put("ms", i);
            agg.add(event, acc);
        }

        long[] expects = Arrays.stream(probabilities).mapToLong(x -> (long) (count * x)).toArray();
        String str = (String) agg.getResult(acc).getMetricsFields().get("ms_his");
        ArrayHistogram his = ArrayHistogram.fromBytes(StringUtils.decodeBase64(str.getBytes(StandardCharsets.UTF_8)));

        for (int i = 0; i < expects.length; i++) {
            long rst = his.getValueAtPercentile(probabilities[i] * 100);
            long expect = expects[i];
            double probability = probabilities[i];
            double error = probability <= 0.01? 0 : Math.abs(rst - expect) / (double) expect;
            System.out.println(String.format("%s:%d,%d,%.4f", probability, expect ,  rst , error));
            assertTrue(error <= 0.05);
        }
    }

    @Test
    public void inputSketch() {
        double[] probabilities = new double[]{0, 0.1, 0.25, 0.5, 0.75, 1};
        initData( "sketch", 2, "binary");
        long count = 100000;
        Map<String, Object> fields = event.getExtractedFields();
        long[] expects = Arrays.stream(probabilities).mapToLong(x -> (long) (count * x)).toArray();

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

        byte[] bytes = (byte[]) agg.getResult(acc).getMetricsFields().get("ms_his");
        ArrayHistogram h = ArrayHistogram.fromBytes(bytes);

        for (int i = 0; i < expects.length; i++) {
            long rst = h.getValueAtPercentile(probabilities[i] * 100);
            long expect = expects[i];
            double probability = probabilities[i];
            double error = probability <= 0.01? 0 : Math.abs(rst - expect) / (double) expect;
            System.out.println(String.format("%s:%d,%d,%.4f", probability, expect ,  rst , error));
            assertTrue(error <= 0.05);
        }
    }

    private void initData(String input_type, int numberOfSignificantValueDigits, String output_format){
        agg = new HdrHistogram();
        UDFContext c = new UDFContext();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("input_type", input_type);
        parameters.put("numberOfSignificantValueDigits", numberOfSignificantValueDigits);
        parameters.put("output_format", output_format);
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