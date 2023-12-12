package com.geedgenetworks.core.utils.KnowlegdeBase;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.config.CommonConfig;
import com.geedgenetworks.common.config.EngineConfig;
import com.geedgenetworks.common.config.KnowledgeConfig;
import com.geedgenetworks.common.utils.HttpClientUtils;
import com.geedgenetworks.core.pojo.KnowLedgeEntity;
import com.geedgenetworks.core.pojo.KnowledgeBaseEntity;
import lombok.Getter;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.geedgenetworks.core.utils.KnowlegdeBase.KnowledgeBaseScheduler.startTask;
import static com.geedgenetworks.core.utils.KnowlegdeBase.KnowledgeBaseScheduler.stopTask;


public class KnowledgeBaseUpdateJob implements Job {


/*
    public static Map<String, KnowledgeConfig> knowledgeConfigMap = new ConcurrentHashMap<>();
    public static  Map<String, List<KnowLedgeEntity>> KnowLedgeEntityMap = new ConcurrentHashMap<>();

*/

    @Getter
    private static  Map<String, KnowledgeBaseEntity> KnowLedgeBaseEntityMap= new ConcurrentHashMap<>() ;
    private static final Logger logger = LoggerFactory.getLogger(KnowledgeBaseUpdateJob.class);

    private static KnowledgeBaseUpdateJob instance;

    private static EngineConfig engineConfig;

    public static synchronized KnowledgeBaseUpdateJob getInstance(RuntimeContext runtimeContext)  {
        if (instance == null) {
            instance = new KnowledgeBaseUpdateJob();
          //  knowledgeBaseClassReflect =CommonConfig.KNOWLEDGE_BASE_CLASS_REFLECT;
            Configuration configuration = (Configuration) runtimeContext
                    .getExecutionConfig().getGlobalJobParameters();
            engineConfig = com.alibaba.fastjson.JSON.parseObject(configuration.toMap().get(Constants.SYSPROP_GROOTSTREAM_CONFIG), EngineConfig.class);
            startTask();
        }
        return instance;
    }

    public static void  updateKnowledgeBase(String name)  {

        try {
            // 获取私有构造方法
/*            Class<?> cls = Class.forName(knowledgeBaseClassReflect.get(knowledgeConfigMap.get(name).getType()));
            Constructor<?> constructor = cls.getDeclaredConstructor();
            constructor.setAccessible(true);
            Method createInstanceMethod = cls.getMethod("getInstance");
            AbstractKnowledgeBase instance = (AbstractKnowledgeBase) createInstanceMethod.invoke(null);
            // 使用实例调用方法
            instance.updateKnowledgeBase(knowledgeConfigMap.get(name));*/


        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }



    public static synchronized  void initKnowledgeBase(String name,AbstractKnowledgeBase abstractKnowledgeBase,RuntimeContext runtimeContext)   {

      if (instance == null) {
            getInstance(runtimeContext);
        }

            if (!KnowLedgeBaseEntityMap.containsKey(name)) {
                List<KnowLedgeEntity> knowLedgeEntityList = new ArrayList<>();
                KnowledgeBaseEntity knowledgeBaseEntity = new KnowledgeBaseEntity();
                knowledgeBaseEntity.setAbstractKnowledgeBase(abstractKnowledgeBase);
                knowledgeBaseEntity.setKnowLedgeEntityList(knowLedgeEntityList);

                try {
                    for(KnowledgeConfig knowledgeConfig : engineConfig.getKnowledgeBaseConfig()){
                        if(name.equals(knowledgeConfig.getName())){
                            knowledgeBaseEntity.setKnowledgeConfig(knowledgeConfig);
                            break;
                        }
                    }
                    KnowLedgeBaseEntityMap.put(name, knowledgeBaseEntity);
                    for (String id : knowledgeBaseEntity.getKnowledgeConfig().getFiles()) {
                      //  KnowLedgeEntity knowLedgeEntity = getMetadata(id);
                        KnowLedgeEntity knowLedgeEntity1 =  new KnowLedgeEntity();
                        knowLedgeEntity1.setIsValid(1);
                        knowLedgeEntity1.setSha256("2a2defd9c96ea5434ac628d3dc4fc6790e30d84c218b02fe798dbc6772dbf76c");
                        knowLedgeEntity1.setPath("http://192.168.44.12:9098/hos/knowledge_base_bucket/84ec56db-8f3b-440a-af79-15148d7cd3c8-YXNuX2J1aWx0aW4=.mmdb");

/*
                        KnowLedgeEntity knowLedgeEntity2 =  new KnowLedgeEntity();
                        knowLedgeEntity2.setIsValid(1);
                        knowLedgeEntity2.setSha256("044228128a1cc7ae0f8d4bae9fdc73eac9dea3e074e7c15796d29167b89cfdee");
                        knowLedgeEntity2.setPath("http://192.168.44.12:9098/hos/knowledge_base_bucket/63220f3e-7792-43a8-9921-d01da16b76b2-aXBfYnVpbHRpbg==.mmdb");
*/

                        knowLedgeEntityList.add(knowLedgeEntity1);
                    }


                    updateKnowledgeBase(name);
                } catch (Exception e) {
                    logger.error("initKnowledgeBase  " + e.getMessage());
                }
            }


    }
    private static KnowLedgeEntity getMetadata(String id) {

        try{
            HttpClientUtils httpClientUtils = new HttpClientUtils();
            String metadate  = httpClientUtils.httpGet(URI.create("/v1/knowledge_base/"+ id +"/meta")) ;
            List<KnowLedgeEntity> knowLedgeEntityList = JSON.parseObject(metadate, new TypeReference<List<KnowLedgeEntity>>() {});
            return knowLedgeEntityList.get(0);

        /*    KnowLedgeEntity knowLedgeEntity = new KnowLedgeEntity();
            knowLedgeEntity.setIsValid(1);
            knowLedgeEntity.setSha256("2a2defd9c96ea5434ac628d3dc4fc6790e30d84c218b02fe798dbc6772dbf76c");
            knowLedgeEntity.setPath("http://192.168.44.12:9098/hos/knowledge_base_hos_bucket/1e8aabe7-2170-445c-9124-6051332ca7d6-YXNuX2J1aWx0aW4=.mmdb");
            return knowLedgeEntity;*/
        } catch (Exception e) {
            logger.error("get file Metadata " + id + " error: " + e.getMessage());
        }

        return null;
    }


    public static synchronized  Boolean  checkUpdateMesssage (String name)  {

       /* Boolean changeFlag = false;
        if(!KnowLedgeEntityMap.get(name).isEmpty()) {

            for (KnowLedgeEntity knowLedgeEntity : KnowLedgeEntityMap.get(name)) {

                KnowLedgeEntity metadata =getMetadata(knowLedgeEntity.getId());
                if(metadata!=null) {
                    if (!knowLedgeEntity.getSha256().equals(metadata.getSha256())) {
                        return true;
                    }
                }
                else {
                    logger.error("get file Metadata " + knowLedgeEntity.getId() + " error " );
                }
            }
        }
        else {
            changeFlag = true;
        }*/
        return true;

    }

    public static void destroyKnowledgeBase() {
        stopTask();
    }

    @Override
    public void execute(JobExecutionContext jobExecutionContext)  {
        for (Map.Entry<String, KnowledgeBaseEntity> entry : KnowLedgeBaseEntityMap.entrySet()) {
            Boolean flag = null;
            try {
                flag = checkUpdateMesssage(entry.getKey());
                if(flag){
                    updateKnowledgeBase(entry.getKey());
                }
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
        }
    }




}
