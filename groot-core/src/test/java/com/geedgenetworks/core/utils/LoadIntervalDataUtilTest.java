package com.geedgenetworks.core.utils;


import java.sql.Timestamp;

public class LoadIntervalDataUtilTest {

    public static void main(String[] args)  throws Exception{
        //testNoError();
        //testNotUpdateDataOnStart();
        //testWithErrorAndNotFail();
        testWithErrorAndFail();
    }

    public static void testNoError()  throws Exception{
        LoadIntervalDataUtil<Timestamp> util = LoadIntervalDataUtil.newInstance(() -> new Timestamp(System.currentTimeMillis()),
                LoadIntervalDataOptions.defaults("time", 3000));

        System.out.println(new Timestamp(System.currentTimeMillis()) + " - " + util.data());

        for (int i = 0; i < 10; i++) {
            Thread.sleep(1000);
            System.out.println(new Timestamp(System.currentTimeMillis()) + " - " + util.data());
        }

        util.stop();
    }

    public static void testNotUpdateDataOnStart()  throws Exception{
        LoadIntervalDataUtil<Timestamp> util = LoadIntervalDataUtil.newInstance(() -> new Timestamp(System.currentTimeMillis()),
                LoadIntervalDataOptions.builder().withName("time").withIntervalMs(3000).withUpdateDataOnStart(false).build());

        System.out.println(new Timestamp(System.currentTimeMillis()) + " - " + util.data());

        for (int i = 0; i < 10; i++) {
            Thread.sleep(1000);
            System.out.println(new Timestamp(System.currentTimeMillis()) + " - " + util.data());
        }

        util.stop();
    }

    public static void testWithErrorAndNotFail() throws Exception{
        final long start = System.currentTimeMillis();
        LoadIntervalDataUtil<Timestamp> util = LoadIntervalDataUtil.newInstance(() -> {
            if(System.currentTimeMillis() - start >= 5000){
                throw new RuntimeException(new Timestamp(System.currentTimeMillis()).toString());
            }
            return new Timestamp(System.currentTimeMillis());
         }, LoadIntervalDataOptions.defaults("time", 3000));

        System.out.println(new Timestamp(System.currentTimeMillis()) + " - " + util.data());

        for (int i = 0; i < 10; i++) {
            Thread.sleep(1000);
            System.out.println(new Timestamp(System.currentTimeMillis()) + " - " + util.data());
        }

        util.stop();
    }

    public static void testWithErrorAndFail() throws Exception{
        final long start = System.currentTimeMillis();
        LoadIntervalDataUtil<Timestamp> util = LoadIntervalDataUtil.newInstance(() -> {
            if(System.currentTimeMillis() - start >= 5000){
                throw new RuntimeException(new Timestamp(System.currentTimeMillis()).toString());
            }
            return new Timestamp(System.currentTimeMillis());
        }, LoadIntervalDataOptions.builder().withName("time").withIntervalMs(3000).withFailOnException(true).build());

        System.out.println(new Timestamp(System.currentTimeMillis()) + " - " + util.data());

        for (int i = 0; i < 10; i++) {
            Thread.sleep(1000);
            System.out.println(new Timestamp(System.currentTimeMillis()) + " - " + util.data());
        }

        util.stop();
    }
}