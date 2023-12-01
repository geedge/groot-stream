package com.geedgenetworks.core.utils;

import com.geedgenetworks.core.pojo.KnowLedgeEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.*;

public class KnowledgeBaseFactory {

    private static Map<Class<?>, Object> knowledgeBaseMap = new HashMap<>();
    private static Map<Class<?>,Map<String, KnowLedgeEntity>> knowledgeEntityMap = new HashMap<>();

    private static Timer timer;
    private static final Logger logger = LoggerFactory.getLogger(KnowledgeBaseFactory.class);

    private static KnowledgeBaseFactory instance;

    public static synchronized KnowledgeBaseFactory getInstance() throws Exception {
        if (instance == null) {
            instance = new KnowledgeBaseFactory();
            startTask();
        }
        return instance;
    }


    public synchronized static <T> T getKnowledgeBase(Class<T> clazz) throws Exception {
        if (knowledgeBaseMap.containsKey(clazz)) {
            return clazz.cast(knowledgeBaseMap.get(clazz));
        } else {
            T knowledgeBase = createKnowledgeBase(clazz);
            knowledgeBaseMap.put(clazz, knowledgeBase);
            return knowledgeBase;
        }
    }

    private  static <T> T createKnowledgeBase(Class<T> clazz) throws Exception {
        // 根据传入的类参数实例化正确的知识库对象
        if (clazz.equals(IPKnowledgeBase.class)) {

            Map<String, KnowLedgeEntity> map = new HashMap<>();
            Map<String, Object> tmpknowledge = new HashMap<>();
            Map<String, String> tmpasn = new HashMap<>();
            tmpasn.put("asn_id","acf1db8589c5e277-2d8cb5cf98593ed8");
            tmpknowledge.put("TSG",tmpasn);
            for (Map.Entry<String, Object> entry : tmpknowledge.entrySet()) {
                Map<String, String> ids = (Map<String, String>) entry.getValue();
                for (Map.Entry<String, String> id : ids.entrySet()) {
                    KnowLedgeEntity knowLedgeEntity = new KnowLedgeEntity();
                    knowLedgeEntity.setId(id.getValue());
                    map.put(id.getKey(),knowLedgeEntity);
                }
            }
            knowledgeEntityMap.put(clazz,map);
            Boolean flag = checkUpdateMesssage(knowledgeEntityMap.get(clazz));

            if(flag){
                IPKnowledgeBase.updateKnowledgeBase(knowledgeEntityMap.get(clazz));
                return clazz.cast(IPKnowledgeBase.getInstance(map));

            }

            return null;
        } else if (clazz.equals(ASNKnowledgeBase.class)) {
            Map<String, KnowLedgeEntity> map = new HashMap<>();
            Map<String, Object> tmpknowledge = new HashMap<>();
            Map<String, String> tmpasn = new HashMap<>();
            tmpasn.put("asn_id","acf1db8589c5e277-2d8cb5cf98593ed8");
            tmpknowledge.put("TSG",tmpasn);
            for (Map.Entry<String, Object> entry : tmpknowledge.entrySet()) {
                Map<String, String> ids = (Map<String, String>) entry.getValue();
                for (Map.Entry<String, String> id : ids.entrySet()) {
                    KnowLedgeEntity knowLedgeEntity = new KnowLedgeEntity();
                    knowLedgeEntity.setId(id.getValue());
                    map.put(id.getKey(),knowLedgeEntity);
                }
            }
            knowledgeEntityMap.put(clazz,map);
            Boolean flag = checkUpdateMesssage(knowledgeEntityMap.get(clazz));

            if(flag){

                ASNKnowledgeBase.updateKnowledgeBase(knowledgeEntityMap.get(clazz));
                return clazz.cast(ASNKnowledgeBase.getInstance(map));

            }

            return null;
        }

        // 如果需要支持更多的知识库类型，可以继续添加判断逻辑

        throw new IllegalArgumentException("Unsupported knowledge base type: " + clazz.getName());
    }

    private  static <T> T updateKnowledgeBase(Class<T> clazz) throws Exception {
        // 根据传入的类参数实例化正确的知识库对象
        if (clazz.equals(IPKnowledgeBase.class)) {
            IPKnowledgeBase.updateKnowledgeBase(knowledgeEntityMap.get(clazz));
        } else if (clazz.equals(ASNKnowledgeBase.class)) {
            ASNKnowledgeBase.updateKnowledgeBase(knowledgeEntityMap.get(clazz));
        }
        throw new IllegalArgumentException("Unsupported knowledge base type: " + clazz.getName());
    }

    private static InputStream downloadFile(String fileUrl, int isValid) {
        InputStream inputStream = null;
        if (isValid == 1) {
            try {
                long start = System.currentTimeMillis();
              //  Header header = new BasicHeader("token", commonConfig.HOS_TOKEN);
                HttpClientUtils httpClientUtils = new HttpClientUtils();
                inputStream = httpClientUtils.httpGetInputStream(fileUrl, 3000);
                long end = System.currentTimeMillis();
                logger.info(
                        "download file " + fileUrl + " finished, speed " + (end - start) + " ms");
            } catch (Exception e) {
                logger.error("download file " + fileUrl + " error: " + e.getMessage());
            }
        }
        return inputStream;
    }
    public static synchronized  Boolean  checkUpdateMesssage (Map<String, KnowLedgeEntity> metaList) {

        Boolean changeFlag = false;
        for (Map.Entry<String, KnowLedgeEntity> entry : metaList.entrySet()) {

            if(!Objects.equals(entry.getValue().getSha256(), "256")){

                InputStream inputStream = downloadFile("http://192.168.44.12:9098/hos/knowledge_base_hos_bucket/1e8aabe7-2170-445c-9124-6051332ca7d6-YXNuX2J1aWx0aW4=.mmdb", 1);
                entry.getValue().setInputStream(inputStream);
                entry.getValue().setIsValid(1);
                entry.getValue().setSha256("256");
                changeFlag = true;
            }
        }

        return changeFlag;

    }




    static class StaticTask extends TimerTask {
        @Override
        public void run() {
            // 执行任务逻辑Map<String, KnowLedgeEntity>
            for (Map.Entry<Class<?>, Map<String, KnowLedgeEntity>> entry : knowledgeEntityMap.entrySet()) {
                Boolean flag = checkUpdateMesssage(entry.getValue());
                if(flag){
                    try {
                        updateKnowledgeBase(entry.getKey());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            System.out.println("Static task is running...");
        }


    }

    public static void startTask() {
        timer = new Timer();
        timer.schedule(new StaticTask(), 0, 10000); // 每隔1秒执行一次任务
    }

    public static void stopTask() {
        if (timer != null) {
            timer.cancel();
            timer = null;
        }
    }



}
