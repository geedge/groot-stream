package com.geedgenetworks.connectors.kafka.rate;

import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;

public class BlockDropRateLimitingStrategy implements RateLimitingStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(BlockDropRateLimitingStrategy.class);
    private static final int blockTsLastSize = 8;
    private static final int blockTsLastMask = 7;
    private final int windowSecond;
    private final long maxRate;
    private final boolean blockEnable;
    private final long maxBlockDuration; // 毫秒
    private final long resetBlockDuration; // 毫秒
    private final SlidingWindowRateStat stat;
    private int blockTsLastIndex;
    private long[] firstBlockTsLast;
    private long[] lastBlockTsLast;
    private long firstBlockTs;
    private long lastBlockTs;
    private boolean hasLimitingStat; // 触发超速限制后, 输出统计信息
    private long limitingStatStartTs;
    private long limitingBlockCount;
    private long limitingBlockMs;
    private long limitingDropCount;
    private long limitingDropSize;

    /**
     * @param maxRate 速率(单位秒)
     * @param maxBlockDuration 最长阻塞时长(毫秒), 值为0代表超速直接丢弃不执行阻塞
     * @param resetBlockDuration 阻塞重置恢复时长, 超速阻塞后速率恢复正常多长时间后重置超速阻塞状态
     */
    public BlockDropRateLimitingStrategy(int windowSecond, long maxRate, long maxBlockDuration, long resetBlockDuration) {
        Preconditions.checkArgument(maxRate > 0);
        Preconditions.checkArgument(maxBlockDuration >= 0);
        Preconditions.checkArgument(resetBlockDuration >= 0);
        this.windowSecond = windowSecond;
        this.maxRate = maxRate;
        this.blockEnable = maxBlockDuration > 0;
        this.maxBlockDuration = maxBlockDuration;
        this.resetBlockDuration = resetBlockDuration;
        this.stat = new SlidingWindowRateStat(windowSecond);
        this.firstBlockTs = 0;
        this.lastBlockTs = 0;
        this.firstBlockTsLast = new long[blockTsLastSize];
        this.lastBlockTsLast = new long[blockTsLastSize];
        this.blockTsLastIndex = blockTsLastMask;
    }

    @Override
    public long maxRate() {
        return maxRate;
    }

    @Override
    public BlockDropRateLimitingStrategy withMaxRate(long maxRate) {
        return new BlockDropRateLimitingStrategy(windowSecond, maxRate, maxBlockDuration, resetBlockDuration);
    }

    @Override
    public RateLimitingStatus record(final long value) {
        final long currentTs = System.currentTimeMillis();
        outLimitingStat(currentTs);

        long currentRate = stat.getCurrentRate(currentTs);
        // ----未超速----
        if (currentRate < maxRate) {
            stat.record(value);
            return RateLimitingStatus.NONE;
        }

        // 输出限速统计信息
        if (!hasLimitingStat) {
            hasLimitingStat = true;
            limitingStatStartTs = currentTs;
            limitingBlockCount = 0;
            limitingBlockMs = 0;
            limitingDropCount = 0;
            limitingDropSize = 0;
        }

        // ----超速----
        // --直接drop--
        if (!blockEnable) {
            limitingDropCount += 1;
            limitingDropSize += value;
            return RateLimitingStatus.DROP;
        }

        // --block后drop--

        // 首次触发 or 重置后首次触发
        if (firstBlockTs == 0L || currentTs - lastBlockTs > resetBlockDuration) {
            //LOG.info("firstBlock");
            firstBlockTs = currentTs;
            lastBlockTs = firstBlockTs;
            blockTsLastIndex = (blockTsLastIndex + 1) & blockTsLastMask;
            firstBlockTsLast[blockTsLastIndex] = firstBlockTs;
            lastBlockTsLast[blockTsLastIndex] = lastBlockTs;
        } else {
            lastBlockTs = currentTs;
            lastBlockTsLast[blockTsLastIndex] = lastBlockTs;

            // drop立马return
            if (lastBlockTs - firstBlockTs > maxBlockDuration) {
                limitingDropCount += 1;
                limitingDropSize += value;
                return RateLimitingStatus.DROP;
            }
        }

        // 需要阻塞, 直到速率下降后
        long ts = currentTs;
        while (currentRate >= maxRate) {
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            ts = System.currentTimeMillis();
            currentRate = stat.getCurrentRate(ts);
        }
        limitingBlockCount += 1;
        limitingBlockMs += ts - currentTs;


        stat.record(value);
        return RateLimitingStatus.BLOCK;
    }

    void outLimitingStat(long currentTs) {
        //if (hasLimitingStat && currentTs - limitingStatStartTs > 300000L) {
        if (hasLimitingStat && currentTs - limitingStatStartTs > 120000L) {
            hasLimitingStat = false;
            StringBuilder blockTsInfos = new StringBuilder();
            if(blockEnable){
                blockTsInfos.append('\n');
                int idx;
                for (int i = 0; i < blockTsLastSize; i++) {
                    idx = (blockTsLastIndex - i) & blockTsLastMask;
                    blockTsInfos.append(new Timestamp(firstBlockTsLast[idx]).toString())
                            .append("_").append(new Timestamp(lastBlockTsLast[idx]).toString()).append("_").append(lastBlockTsLast[idx] - firstBlockTsLast[idx]);
                    if(i != blockTsLastSize - 1){
                        blockTsInfos.append(",\n");
                    }
                }
            }
            LOG.info("from {} to {} limiting info for max rate({} bytes), blockCount: {}, blockMs: {}, dropCount: {}, dropSize: {},blockTsInfos:{}.", new Timestamp(limitingStatStartTs).toString(), new Timestamp(currentTs).toString(),
                    maxRate,limitingBlockCount, limitingBlockMs, limitingDropCount, limitingDropSize, blockTsInfos.toString()
            );
        }
    }

    @Override
    public String toString() {
        return "BlockDropRateLimitingStrategy{" +
                "maxRate=" + maxRate +
                " bytes/second, maxBlockDuration=" + maxBlockDuration +
                " ms, resetBlockDuration=" + resetBlockDuration +
                " ms}";
    }
}
