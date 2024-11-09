package com.geedgenetworks.core.connector.schema.utils;

import com.geedgenetworks.spi.table.schema.DynamicSchema;
import com.geedgenetworks.spi.table.schema.HttpDynamicSchema;
import com.geedgenetworks.spi.table.schema.SchemaChangeAware;
import com.geedgenetworks.spi.table.schema.SchemaParser;
import com.geedgenetworks.spi.table.type.StructType;
import com.geedgenetworks.spi.table.type.Types;
import org.apache.flink.util.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class DynamicSchemaManagerTest {
    static final Logger LOG = LoggerFactory.getLogger(DynamicSchemaManagerTest.class.getSimpleName());

    public static void main(String[] args) throws Exception{
        //testOneThread();
        //testMultiThread();
        testMultiThreadForHttpDynamicSchema();
    }

    public static void testOneThread() throws Exception{
        RandomDynamicSchema schema1 = new RandomDynamicSchema( 1000 * 5, "Schema1", 0.25);
        RandomDynamicSchema schema11 = new RandomDynamicSchema( 1000 * 5, "Schema1", 0.25);
        RandomDynamicSchema schema2 = new RandomDynamicSchema( 1000 * 10, "Schema1", 0.9);

        LOG.info("start");
        PrintSchemaChangeAware aware1 = new PrintSchemaChangeAware("aware1");
        PrintSchemaChangeAware aware2 = new PrintSchemaChangeAware("aware2");
        PrintSchemaChangeAware aware11 = new PrintSchemaChangeAware("aware11");
        PrintSchemaChangeAware aware22 = new PrintSchemaChangeAware("aware22");

        schema1.registerSchemaChangeAware(aware1);
        schema1.registerSchemaChangeAware(aware2);
        schema1.registerSchemaChangeAware(aware1);
        schema1.registerSchemaChangeAware(aware2);

        schema11.registerSchemaChangeAware(aware11);
        schema11.registerSchemaChangeAware(aware22);


        schema2.registerSchemaChangeAware(aware1);
        schema2.registerSchemaChangeAware(aware2);
        schema2.registerSchemaChangeAware(aware1);
        schema2.registerSchemaChangeAware(aware2);

        Thread.sleep(1000 * 60 * 2);
        schema1.unregisterSchemaChangeAware(aware1);
        schema1.unregisterSchemaChangeAware(aware2);
        schema1.unregisterSchemaChangeAware(aware11);
        schema1.unregisterSchemaChangeAware(aware22);

        Thread.sleep(1000 * 20);
        schema2.unregisterSchemaChangeAware(aware1);
        schema2.unregisterSchemaChangeAware(aware2);

        Thread.sleep(1000 * 3);


        schema1.registerSchemaChangeAware(aware2);
        schema2.registerSchemaChangeAware(aware1);
        Thread.sleep(1000 * 60 * 1);
        schema1.unregisterSchemaChangeAware(aware2);
        schema2.unregisterSchemaChangeAware(aware1);
        Thread.sleep(1000 * 3);
    }

    public static void testMultiThreadForHttpDynamicSchema() throws Exception{
        LOG.info("start");

        List<Thread> threads = new ArrayList<>(10);
        Thread thread;
        for (int i = 0; i < 5; i++) {
            int finalI = i + 1;
            thread = new Thread(() -> {
                DynamicSchema schema1 = new HttpDynamicSchema( "http://127.0.0.1/session_record_schema.json", SchemaParser.PARSER_AVRO, 1000 * 5);
                System.out.println(schema1.getDataType());
                PrintSchemaChangeAware aware1 = new PrintSchemaChangeAware("aware1_" + finalI);
                schema1.registerSchemaChangeAware(aware1);
                try {
                    Thread.sleep(1000 * 60 * 1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                schema1.unregisterSchemaChangeAware(aware1);
            });
            threads.add(thread);

            thread = new Thread(() -> {
                DynamicSchema schema2 = new HttpDynamicSchema( "http://127.0.0.1/session_record_schema.json", SchemaParser.PARSER_AVRO, 1000 * 5);
                System.out.println(schema2.getDataType());
                PrintSchemaChangeAware aware2 = new PrintSchemaChangeAware("aware2_" + finalI);
                schema2.registerSchemaChangeAware(aware2);
                try {
                    Thread.sleep(1000 * 60 * 1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                schema2.unregisterSchemaChangeAware(aware2);
            });
            threads.add(thread);
        }

        for (int i = 0; i < threads.size(); i++) {
            thread = threads.get(i);
            thread.start();
        }

        for (int i = 0; i < threads.size(); i++) {
            thread = threads.get(i);
            thread.join();
        }
        Thread.sleep(1000 * 3);
        LOG.info("done");
    }

    public static void testMultiThread() throws Exception{
        LOG.info("start");

        List<Thread> threads = new ArrayList<>(10);
        Thread thread;
        for (int i = 0; i < 5; i++) {
            int finalI = i + 1;
            thread = new Thread(() -> {
                RandomDynamicSchema schema1 = new RandomDynamicSchema( 1000 * 5, "Schema1", 0.25);
                PrintSchemaChangeAware aware1 = new PrintSchemaChangeAware("aware1_" + finalI);
                schema1.registerSchemaChangeAware(aware1);
                try {
                    Thread.sleep(1000 * 60 * 1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                schema1.unregisterSchemaChangeAware(aware1);
            });
            threads.add(thread);

            thread = new Thread(() -> {
                RandomDynamicSchema schema2 = new RandomDynamicSchema(1000 * 10, "Schema1", 0.9);
                PrintSchemaChangeAware aware2 = new PrintSchemaChangeAware("aware2_" + finalI);
                schema2.registerSchemaChangeAware(aware2);
                try {
                    Thread.sleep(1000 * 60 * 1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                schema2.unregisterSchemaChangeAware(aware2);
            });
            threads.add(thread);
        }

        for (int i = 0; i < threads.size(); i++) {
            thread = threads.get(i);
            thread.start();
        }

        for (int i = 0; i < threads.size(); i++) {
            thread = threads.get(i);
            thread.join();
        }
        Thread.sleep(1000 * 3);
        LOG.info("done");
    }

    public static class PrintSchemaChangeAware implements SchemaChangeAware {
        private final String name;

        public PrintSchemaChangeAware(String name) {
            this.name = name;
        }

        @Override
        public void schemaChange(StructType dataType) {
            String info = String.format("%s receive schema change:%s", name, dataType);
            //System.out.println(info);
            LOG.info(info);
        }

        @Override
        public String toString() {
            return name;
        }
    }

    public static class RandomDynamicSchema extends DynamicSchema{
        private final String key;
        private final double probability;

        public RandomDynamicSchema(long intervalMs, String identifier, double probability) {
            super(null, intervalMs);
            this.key = identifier + "_" + TimeUtils.formatWithHighestUnit(Duration.ofMillis(intervalMs));
            this.probability = probability;
        }

        @Override
        public String getCacheKey() {
            return key;
        }

        @Override
        protected String getDataTypeContent() {
            return null;
        }

        @Override
        public StructType updateDataType() {
            if(ThreadLocalRandom.current().nextDouble() < probability){
                return (StructType) Types.parseDataType(String.format("struct<name_%s: string>", key));
            }
            return null;
        }

    }
}