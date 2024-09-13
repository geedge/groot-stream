package com.geedgenetworks.connectors.mock.faker;

public class SequenceFaker extends Faker<Long> {
    private final long start;
    private final long step;
    private final int batch;
    private long value;
    private int cnt;

    public SequenceFaker(long start) {
        this(start, 1, 1);
    }

    public SequenceFaker(long start, long step) {
        this(start, step, 1);
    }

    public SequenceFaker(long start, long step, int batch) {
        this.start = start;
        this.step = step;
        this.batch = batch;
        this.value = start;
        this.cnt = 0;
    }

    @Override
    public Long geneValue() throws Exception {
        Long rst = value;
        cnt++;
        if(cnt == batch){
            cnt = 0;
            value += step;
        }
        return rst;
    }

}
