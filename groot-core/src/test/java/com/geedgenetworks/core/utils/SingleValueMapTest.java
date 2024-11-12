package com.geedgenetworks.core.utils;

import com.geedgenetworks.api.configuration.util.LoadIntervalDataOptions;
import com.geedgenetworks.api.configuration.util.LoadIntervalDataUtil;
import org.junit.jupiter.api.Assertions;

import java.sql.Timestamp;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class SingleValueMapTest {

    public static void main(String[] args) throws Exception {
        //testSingleValue();
        testSingleValueWithLoadIntervalDataUtil();
    }

    public static void testSingleValue() throws Exception {
        Thread[] threads = new Thread[20];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                SingleValueMap.Data<ConnDada> connDada = null;
                try {
                    connDada = SingleValueMap.acquireData("conn_data", () -> new ConnDada(), x -> {
                        System.out.println("close conn");
                    });
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                try {
                    Thread.sleep(ThreadLocalRandom.current().nextInt(5) * 10);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                connDada.release();
            }, "Thread-" + i);
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i].start();
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }

        System.out.println("initCnt:" + ConnDada.initCnt.get());
        Assertions.assertEquals(ConnDada.initCnt.get(), 1);
    }

    public static void testSingleValueWithLoadIntervalDataUtil() throws Exception {
        Thread[] threads = new Thread[20];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() -> {
                SingleValueMap.Data<LoadIntervalDataUtil<Timestamp>> util = null;
                try {
                    util = SingleValueMap.acquireData("LoadIntervalDataUtil",
                            () ->  LoadIntervalDataUtil.newInstance(() -> new Timestamp(System.currentTimeMillis()), LoadIntervalDataOptions.defaults("time", 3000)),
                            LoadIntervalDataUtil::stop);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }


                try {
                    for (int j = 0; j < 10; j++) {
                        Thread.sleep(1000);
                        System.out.println(Thread.currentThread().getName() + " - " + new Timestamp(System.currentTimeMillis()) + " - " + util.getData().data());
                    }

                    Thread.sleep(ThreadLocalRandom.current().nextInt(5) * 10);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                util.release();
            }, "Thread-" + i);
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i].start();
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }

    }

    public static class ConnDada {
        static AtomicInteger initCnt = new AtomicInteger(0);
        public ConnDada(){
            System.out.println("ConnDada init");
            initCnt.incrementAndGet();
        }

    }
}