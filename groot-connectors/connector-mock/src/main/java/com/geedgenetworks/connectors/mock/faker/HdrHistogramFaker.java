package com.geedgenetworks.connectors.mock.faker;

import com.geedgenetworks.sketch.util.StringUtils;
import org.HdrHistogram.HistogramSketch;

import java.util.concurrent.ThreadLocalRandom;

public class HdrHistogramFaker extends Faker<Object> {
    private final int max;
    private final int batchCount;
    private final int numberOfSignificantValueDigits;
    private HistogramSketch his;

    public HdrHistogramFaker(int max, int batchCount, int numberOfSignificantValueDigits) {
        this.max = max;
        this.batchCount = batchCount;
        this.numberOfSignificantValueDigits = numberOfSignificantValueDigits;
    }

    @Override
    public void init(int instanceCount, int instanceIndex) throws Exception {
        his = new HistogramSketch(1L, max, numberOfSignificantValueDigits, false);
    }

    @Override
    public Object geneValue() throws Exception {
        his.reset();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = 0; i < batchCount; i++) {
            his.recordValue(random.nextInt(max));
        }
        return StringUtils.encodeBase64String(his.toBytes());
    }

}
