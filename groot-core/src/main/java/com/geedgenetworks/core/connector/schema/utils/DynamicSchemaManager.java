package com.geedgenetworks.core.connector.schema.utils;

import com.geedgenetworks.core.connector.schema.DynamicSchema;
import com.geedgenetworks.core.connector.schema.SchemaChangeAware;
import com.geedgenetworks.core.types.StructType;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;
public class DynamicSchemaManager {
    private static final Logger LOG = LoggerFactory.getLogger(DynamicSchemaManager.class);
    private static final Map<String, DynamicSchemaWithAwares> registeredSchemaWithAwares = new LinkedHashMap<>();
    private static ScheduledExecutorService scheduler = null;

    // 注册某个dynamicSchema的监听感知
    public static synchronized void registerSchemaChangeAware(DynamicSchema dynamicSchema, SchemaChangeAware aware){
        checkNotNull(dynamicSchema);
        checkNotNull(aware);

        String key = dynamicSchema.getCacheKey();
        DynamicSchemaWithAwares schemaWithAwares = registeredSchemaWithAwares.get(key);
        if(schemaWithAwares == null){
            schemaWithAwares = new DynamicSchemaWithAwares(dynamicSchema);
            schedule(schemaWithAwares);
            registeredSchemaWithAwares.put(key, schemaWithAwares);
            LOG.info("start schedule for {}, current contained schedules:{}", schemaWithAwares.dynamicSchema.getCacheKey(), registeredSchemaWithAwares.keySet());
        }

        for (SchemaChangeAware registeredAware : schemaWithAwares.awares) {
            if(registeredAware == aware){
                LOG.error("aware({}) for {} has already registered", aware, key);
                return;
            }
        }

        schemaWithAwares.awares.add(aware);
        LOG.info("register aware({}) for {}", aware, key);
    }

    // 注销某个dynamicSchema的监听感知
    public static synchronized void unregisterSchemaChangeAware(DynamicSchema dynamicSchema, SchemaChangeAware aware){
        checkNotNull(dynamicSchema);
        checkNotNull(aware);

        String key = dynamicSchema.getCacheKey();
        DynamicSchemaWithAwares schemaWithAwares = registeredSchemaWithAwares.get(key);
        if(schemaWithAwares == null){
            LOG.error("not register aware for {}", key);
            return;
        }

        Iterator<SchemaChangeAware> iter = schemaWithAwares.awares.iterator();
        SchemaChangeAware registeredAware;
        boolean find = false;
        while (iter.hasNext()){
            registeredAware = iter.next();
            if(registeredAware == aware){
                iter.remove();
                find = true;
                break;
            }
        }

        if(find){
            LOG.info("unregister aware({}) for {}", aware, schemaWithAwares.dynamicSchema.getCacheKey());
            if(schemaWithAwares.awares.isEmpty()){
                registeredSchemaWithAwares.remove(key);
                LOG.info("stop schedule for {}, current contained schedules:{}", schemaWithAwares.dynamicSchema.getCacheKey(), registeredSchemaWithAwares.keySet());
            }
            if(registeredSchemaWithAwares.isEmpty()){
                destroySchedule();
            }
        }else{
            LOG.error("can not find register aware({}) for {}", aware, schemaWithAwares.dynamicSchema.getCacheKey());
        }
    }

    private static void schedule(DynamicSchemaWithAwares schemaWithAwares){
        if(scheduler == null){
            scheduler = Executors.newScheduledThreadPool(1, new ExecutorThreadFactory("DynamicSchemaUpdateScheduler"));
            LOG.info("create SchemaUpdateScheduler");
        }
        scheduler.schedule(schemaWithAwares, schemaWithAwares.dynamicSchema.getIntervalMs(), TimeUnit.MILLISECONDS);
    }

    private static void destroySchedule(){
        if(scheduler != null){
            try {
                scheduler.shutdownNow();
                LOG.info("destroy SchemaUpdateScheduler");
            } catch (Exception e) {
                LOG.error("shutdown error", e);
            }
            scheduler = null;
        }
    }

    private static class DynamicSchemaWithAwares implements Runnable{
        DynamicSchema dynamicSchema;
        private List<SchemaChangeAware> awares;

        public DynamicSchemaWithAwares(DynamicSchema dynamicSchema) {
            this.dynamicSchema = dynamicSchema;
            awares = new ArrayList<>();
        }

        @Override
        public void run() {
            if(awares.isEmpty()){
                return;
            }

            try {
                update();
            } catch (Throwable e) {
                LOG.error("schema update error", e);
            }

            if(!awares.isEmpty()){
                scheduler.schedule(this, dynamicSchema.getIntervalMs(), TimeUnit.MILLISECONDS);
            }
        }

        public void update() {
            StructType dataType = dynamicSchema.updateDataType();
            // 距离上次没有更新
            if(dataType == null){
                return;
            }

            LOG.warn("schema for {} change to:{}", dynamicSchema.getCacheKey(), dataType.simpleString());
            for (SchemaChangeAware aware : awares) {
                try {
                    aware.schemaChange(dataType);
                } catch (Exception e) {
                    LOG.error("schema change aware error", e);
                }
            }
        }

    }
}
