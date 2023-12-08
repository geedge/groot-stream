package com.geedgenetworks.core.utils.KnowlegdeBase;

import com.geedgenetworks.common.config.CommonConfig;
import com.geedgenetworks.common.config.KnowledgeConfig;
import com.geedgenetworks.core.pojo.KnowLedgeEntity;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;



public class KnowledgeBaseSchedule {

    private static Timer timer;

    public static Map<String, KnowledgeConfig> knowledgeConfigMap = new ConcurrentHashMap<>();
    public static  Map<String, List<KnowLedgeEntity>> KnowLedgeEntityMap = new ConcurrentHashMap<>();
    @Getter
    private static  Map<String, String> knowledgeBaseClassReflect= new ConcurrentHashMap<>() ;
    private static final Logger logger = LoggerFactory.getLogger(KnowledgeBaseSchedule.class);

    private static KnowledgeBaseSchedule instance;

    public static synchronized KnowledgeBaseSchedule getInstance()  {
        if (instance == null) {
            instance = new KnowledgeBaseSchedule();
            knowledgeBaseClassReflect =CommonConfig.KNOWLEDGE_BASE_CLASS_REFLECT;
            ScheduTask.startTask();
        }
        return instance;
    }

    public static void  updateKnowledgeBase(String name)  {

        try {
            // 获取私有构造方法
            Class<?> cls = Class.forName(knowledgeBaseClassReflect.get(knowledgeConfigMap.get(name).getType()));
            Constructor<?> constructor = cls.getDeclaredConstructor();
            constructor.setAccessible(true);
          //  knowledgeUtil instance = (knowledgeUtil) constructor.newInstance();

            Method createInstanceMethod = cls.getMethod("getInstance");
            AbstractKnowledgeBase instance = (AbstractKnowledgeBase) createInstanceMethod.invoke(null);
            // 使用实例调用方法
            instance.updateKnowledgeBase(knowledgeConfigMap.get(name));
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }



    public static synchronized  void initKnowledgeBase(KnowledgeConfig KnowledgeConfig)   {

        if (instance == null) {
            getInstance();
        }
            if (!knowledgeConfigMap.containsKey(KnowledgeConfig.getName())) {
                List<KnowLedgeEntity> knowLedgeEntityList = new ArrayList<>();
                try {
                    knowledgeConfigMap.put(KnowledgeConfig.getName(), KnowledgeConfig);
                    updateKnowledgeBase(KnowledgeConfig.getName());
                    for (String id : KnowledgeConfig.getFiles()) {
                        KnowLedgeEntity knowLedgeEntity = getMetadata(id);
                        knowLedgeEntityList.add(knowLedgeEntity);
                    }
                    KnowLedgeEntityMap.put(KnowledgeConfig.getName(), knowLedgeEntityList);
                } catch (Exception e) {
                    logger.error("initKnowledgeBase  " + KnowledgeConfig.getName() + " error: " + e.getMessage());
                }
                KnowLedgeEntityMap.put(KnowledgeConfig.getName(), knowLedgeEntityList);
            }


    }
    private static KnowLedgeEntity getMetadata(String id) {

        try{
/*          暂时写死
            HttpClientUtils httpClientUtils = new HttpClientUtils();
            String metadate  = httpClientUtils.httpGet(URI.create("/v1/knowledge_base/"+ id +"/meta")) ;
            List<KnowLedgeEntity> knowLedgeEntityList = JSON.parseObject(metadate, new TypeReference<List<KnowLedgeEntity>>() {});
            return knowLedgeEntityList.get(0);
*/
            KnowLedgeEntity knowLedgeEntity = new KnowLedgeEntity();
            knowLedgeEntity.setIsValid(1);
            knowLedgeEntity.setSha256("2a2defd9c96ea5434ac628d3dc4fc6790e30d84c218b02fe798dbc6772dbf76c");
            knowLedgeEntity.setPath("http://192.168.44.12:9098/hos/knowledge_base_hos_bucket/1e8aabe7-2170-445c-9124-6051332ca7d6-YXNuX2J1aWx0aW4=.mmdb");
            return knowLedgeEntity;
        } catch (Exception e) {
            logger.error("get file Metadata " + id + " error: " + e.getMessage());
        }

        return null;
    }


    public static synchronized  Boolean  checkUpdateMesssage (String name) throws NoSuchAlgorithmException {

        //暂时写死Boolean changeFlag = false;
        Boolean changeFlag = true;
        if(!KnowLedgeEntityMap.get(name).isEmpty()) {

            for (KnowLedgeEntity knowLedgeEntity : KnowLedgeEntityMap.get(name)) {

                if (!knowLedgeEntity.getSha256().equals(getMetadata(knowLedgeEntity.getId()).getSha256())) {
                    return true;

                }
            }
        }
        else {
            changeFlag = true;
        }
        return changeFlag;

    }






    static  class ScheduTask extends TimerTask {


        @Override
        public void run() {
            for (Map.Entry<String, KnowledgeConfig> entry : knowledgeConfigMap.entrySet()) {
                Boolean flag = null;
                try {
                    flag = checkUpdateMesssage(entry.getValue().getName());
                    if(flag){
                        updateKnowledgeBase(entry.getKey());
                    }
                } catch (NoSuchAlgorithmException e) {
                    logger.error(e.getMessage());
                } catch (Exception e) {
                    logger.error(e.getMessage());
                }
            }
        }

        public static void startTask() {
            timer = new Timer();
            timer.schedule(new ScheduTask(), 0, 300000); // 每隔300秒执行一次任务
        }

        public static void stopTask() {
            if (timer != null) {
                timer.cancel();
                timer = null;
            }
        }
    }

}
