package com.geedgenetworks.connectors.kafka.rate;

import com.geedgenetworks.connectors.kafka.rate.SlidingWindowRateStat;

public class SlidingWindowRateStatTest {

    public static void main(String[] args) throws Exception{
        //testRate();
        testRateLimit();
    }

    public static void testRate() throws Exception{
        SlidingWindowRateStat stat = new SlidingWindowRateStat();

        System.out.println(new java.sql.Timestamp(System.currentTimeMillis()));

        int times = 500 * 60;
        long startTs = System.currentTimeMillis();
        long ts;
        long diff;
        for (int i = 0; i < times; i++) {
            if(i < 500 * 30){
                stat.record(2000);
            }
            ts = System.currentTimeMillis();
            if(i % 50 == 0){
                System.out.println(new java.sql.Timestamp(System.currentTimeMillis()) + ":" + stat.getCurrentRate());
            }
            diff = startTs + i * 2 - ts;
            if(diff > 0 ){
                Thread.sleep(diff);
            }
        }

    }

    public static void testRateLimit() throws Exception{
        SlidingWindowRateStat stat = new SlidingWindowRateStat();

        System.out.println(new java.sql.Timestamp(System.currentTimeMillis()));

        int times = 500 * 60;
        long startTs = System.currentTimeMillis();
        long ts;
        long diff;
        long rate;
        boolean limit = false;
        for (int i = 0; i < times; i++) {
            rate = stat.getCurrentRate();
            if(rate < 600000){
                if(limit == true){
                    limit = false;
                    System.out.println(new java.sql.Timestamp(System.currentTimeMillis()) + "," + rate  +", limit:" + i);
                }
                if(i < 500 * 30){
                    stat.record(2000);
                }
            }else{
                if(limit == false){
                    limit = true;
                    System.out.println(new java.sql.Timestamp(System.currentTimeMillis()) + "," + rate  +", limit:" + i);
                }
            }

            ts = System.currentTimeMillis();
            if(i % 50 == 0){
                System.out.println(new java.sql.Timestamp(System.currentTimeMillis()) + ":" + stat.getCurrentRate());
            }
            diff = startTs + i * 2 - ts;
            if(diff > 0 ){
                Thread.sleep(diff);
            }
        }

    }
}
