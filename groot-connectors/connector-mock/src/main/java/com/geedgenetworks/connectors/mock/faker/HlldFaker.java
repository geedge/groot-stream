package com.geedgenetworks.connectors.mock.faker;

import com.geedgenetworks.sketch.hlld.Hll;
import com.geedgenetworks.sketch.util.StringUtils;

import java.util.concurrent.ThreadLocalRandom;

public class HlldFaker extends Faker<Object> {
    private final long itemCount;
    private final int batchCount;
    private final int precision;
    private Hll hll;

    public HlldFaker(long itemCount, int batchCount, int precision) {
        this.itemCount = itemCount;
        this.batchCount = batchCount;
        this.precision = precision;
    }

    public HlldFaker(long itemCount, int batchCount) {
        this(itemCount, batchCount, 12);
    }

    @Override
    public void init(int instanceCount, int instanceIndex) throws Exception {
        hll = new Hll(precision);
    }

    @Override
    public Object geneValue() throws Exception {
        hll.reset();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = 0; i < batchCount; i++) {
            hll.add(random.nextLong(itemCount));
        }
        return StringUtils.encodeBase64String(hll.toBytes());
    }

}
