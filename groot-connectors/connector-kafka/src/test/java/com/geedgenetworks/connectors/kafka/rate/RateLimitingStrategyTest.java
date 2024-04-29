package com.geedgenetworks.connectors.kafka.rate;

import org.apache.flink.util.TimeUtils;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class RateLimitingStrategyTest {

    public static void main(String[] args) throws Exception {
        //testNoRateLimitingStrategy();
        //testBlockDropRateLimitingStrategyForDrop();
        //testBlockDropRateLimitingStrategyForBlockDrop();
        //testBlockDropRateLimitingStrategyForBlock();

        testBlockDropRateLimitingStrategyForDropMultiThread();
    }

    public static void testNoRateLimitingStrategy() throws Exception{
        RateLimitingStrategy strategy = new NoRateLimitingStrategy();

        int times = 500 * 60;
        long startTs = System.currentTimeMillis();
        long ts;
        long lastTs = startTs;
        long diff;
        long pass = 0;
        long drop = 0;
        for (int i = 0; i < times; i++) {
            if(i < 500 * 30){
                long value = 2000;
                RateLimitingStatus status = strategy.record(value);
                if(status == RateLimitingStatus.DROP){
                    drop += value;
                }else{
                    pass += value;
                }
            }
            ts = System.currentTimeMillis();
            if(ts - lastTs >= 100){
                lastTs = ts;
                System.out.println(new java.sql.Timestamp(System.currentTimeMillis()) + ":" + pass + ", " + drop);
            }
            diff = startTs + i * 2 - ts;
            if(diff > 0 ){
                Thread.sleep(diff);
            }
        }

    }

    public static void testBlockDropRateLimitingStrategyForDrop() throws Exception{
        BlockDropRateLimitingStrategy strategy = new BlockDropRateLimitingStrategy(600000, 0, 0 );

        int times = 500 * 60;
        long startTs = System.currentTimeMillis();
        long ts;
        long lastTs = startTs;
        long diff;
        long pass = 0;
        long drop = 0;
        for (int i = 0; i < times; i++) {
            if(i < 500 * 30){
                long value = 2000;
                RateLimitingStatus status = strategy.record(value);
                if(status == RateLimitingStatus.DROP){
                    drop += value;
                }else{
                    pass += value;
                }
            }
            ts = System.currentTimeMillis();
            if(ts - lastTs >= 100){
                lastTs = ts;
                System.out.println(new java.sql.Timestamp(System.currentTimeMillis()) + ":" + pass + ", " + drop);
            }
            diff = startTs + i * 2 - ts;
            if(diff > 0 ){
                Thread.sleep(diff);
            }
        }


        strategy.outLimitingStat(System.currentTimeMillis() + 300000L);
    }

    public static void testBlockDropRateLimitingStrategyForBlockDrop() throws Exception{
        //BlockDropRateLimitingStrategy strategy = new BlockDropRateLimitingStrategy(600000, 10000, 1000 );
        BlockDropRateLimitingStrategy strategy = new BlockDropRateLimitingStrategy(600000, 40000, 1000 );

        int times = 500 * 60;
        long startTs = System.currentTimeMillis();
        long ts;
        long lastTs = startTs;
        long diff;
        long diffSum = 0;
        long pass = 0;
        long drop = 0;
        for (int i = 0; i < times; i++) {
            ts = System.currentTimeMillis();
            if(ts - lastTs >= 100){
                lastTs = ts;
                System.out.println(new java.sql.Timestamp(System.currentTimeMillis()) + ":" + pass + ", " + drop);
            }
            diff = startTs + (i + 1) * 2 - ts;
            if(diff > 0 ){
                if(i < 500 * 30){
                    diffSum += diff;
                }
                Thread.sleep(diff);
            }

            if(i < 500 * 30){
                long value = 2000;
                RateLimitingStatus status = strategy.record(value);
                if(status == RateLimitingStatus.DROP){
                    drop += value;
                }else{
                    pass += value;
                }
            }
        }

        System.out.println("diffSum:" + diffSum);
        strategy.outLimitingStat(System.currentTimeMillis() + 300000L);
    }

    public static void testBlockDropRateLimitingStrategyForBlock() throws Exception{
        //RateLimitingStrategy strategy = new BlockDropRateLimitingStrategy(600000, 10000, 1000 );
        BlockDropRateLimitingStrategy strategy = new BlockDropRateLimitingStrategy(600000, TimeUtils.parseDuration("1d").toMillis(), 1000 );

        int times = 500 * 60;
        long startTs = System.currentTimeMillis();
        long ts;
        long lastTs = startTs;
        long diff;
        long diffSum = 0;
        long pass = 0;
        long drop = 0;
        for (int i = 0; i < times; i++) {
            ts = System.currentTimeMillis();
            if(ts - lastTs >= 100){
                lastTs = ts;
                System.out.println(new java.sql.Timestamp(System.currentTimeMillis()) + ":" + pass + ", " + drop);
            }
            diff = startTs + (i + 1) * 2 - ts;
            if(diff > 0 ){
                if(i < 500 * 30){
                    diffSum += diff;
                }
                Thread.sleep(diff);
            }

            if(i < 500 * 30){
                long value = 2000;
                RateLimitingStatus status = strategy.record(value);
                if(status == RateLimitingStatus.DROP){
                    drop += value;
                }else{
                    pass += value;
                }
            }

        }

        System.out.println("diffSum:" + diffSum);
        strategy.outLimitingStat(System.currentTimeMillis() + 300000L);
    }


    public static void testBlockDropRateLimitingStrategyForDropMultiThread() throws Exception{
        final ArrayBlockingQueue<Long> queue = new ArrayBlockingQueue<>(1000000);

        Thread producerThread = new Thread(() -> {
            int times = 500 * 60;
            long startTs = System.currentTimeMillis();
            long ts;
            long diff;
            for (int i = 0; i < times; i++) {
                ts = System.currentTimeMillis();
                diff = startTs + (i + 1) * 2 - ts;
                if(diff > 0 ){
                    try {
                        Thread.sleep(diff);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }

                if(i < 500 * 30){
                    long value = 2000;
                    try {
                        queue.put(value);
                        //System.out.println(new java.sql.Timestamp(System.currentTimeMillis()) + ",put");
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }

            System.out.println(new java.sql.Timestamp(System.currentTimeMillis()) + "producerThread end");
        });

        Thread consumerThread = new Thread(() -> {
            //BlockDropRateLimitingStrategy strategy = new BlockDropRateLimitingStrategy(600000, 40000, 1000 );
            BlockDropRateLimitingStrategy strategy = new BlockDropRateLimitingStrategy(600000, 30000, 1000 );
            //BlockDropRateLimitingStrategy strategy = new BlockDropRateLimitingStrategy(600000, 0, 1000 );
            //BlockDropRateLimitingStrategy strategy = new BlockDropRateLimitingStrategy(600000, TimeUtils.parseDuration("1d").toMillis(), 1000 );
            System.out.println(new java.sql.Timestamp(System.currentTimeMillis()) + ",poll_before");
            long startTs = System.currentTimeMillis();
            long ts;
            long lastTs = startTs;
            long pass = 0;
            long drop = 0;
            int i = 1;
            long diff;
            while (true){
                try {
                    Long value = queue.poll(60, TimeUnit.MICROSECONDS);
                    //System.out.println(new java.sql.Timestamp(System.currentTimeMillis()) + ",poll");
                    if(value == null){
                        break;
                    }

                    ts = System.currentTimeMillis();

                    diff = startTs + (i + 1) * 2 * 12 / 6  - ts;
                    //diff = startTs + (i + 1) * 2  - ts;
                    //diff = startTs + (i + 1) * 2* 11 / 6  - ts;
                    if(diff > 0 ){
                        try {
                            Thread.sleep(diff);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    i++;

                    if(ts - lastTs >= 100){
                        lastTs = ts;
                        System.out.println(new java.sql.Timestamp(System.currentTimeMillis()) + ":" + pass + ", " + drop);
                    }

                    RateLimitingStatus status = strategy.record(value);
                    if(status == RateLimitingStatus.DROP){
                        drop += value;
                    }else{
                        pass += value;
                    }
                } catch (InterruptedException e) {
                    break;
                }
            }

            System.out.println(new java.sql.Timestamp(System.currentTimeMillis()) + ":" + pass + ", " + drop);
            strategy.outLimitingStat(System.currentTimeMillis() + 300000L);
        });

        producerThread.start();
        consumerThread.start();

        producerThread.join();
        consumerThread.join();
    }

}