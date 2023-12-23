package com.geedgenetworks.core.utils.KnowlegdeBase;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.config.CommonConfig;
import com.geedgenetworks.common.config.KnowledgeBaseConfig;
import com.geedgenetworks.core.pojo.KnowLedgeFileEntity;
import com.geedgenetworks.core.pojo.KnowledgeBaseEntity;
import com.geedgenetworks.core.utils.HttpClientPoolUtil;
import lombok.Getter;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.geedgenetworks.core.utils.KnowlegdeBase.KnowledgeBaseScheduler.startTask;
import static com.geedgenetworks.core.utils.KnowlegdeBase.KnowledgeBaseScheduler.stopTask;


public class KnowledgeBaseUpdateJob implements Job {


    @Getter
    private static  Map<String, KnowledgeBaseEntity> KnowLedgeBaseEntityMap= new ConcurrentHashMap<>() ;
    private static final Logger logger = LoggerFactory.getLogger(KnowledgeBaseUpdateJob.class);

    private static KnowledgeBaseUpdateJob instance;

    private static CommonConfig engineConfig;

    public static synchronized KnowledgeBaseUpdateJob getInstance(RuntimeContext runtimeContext)  {
        if (instance == null) {
            instance = new KnowledgeBaseUpdateJob();
            Configuration configuration = (Configuration) runtimeContext
                    .getExecutionConfig().getGlobalJobParameters();
            engineConfig = com.alibaba.fastjson.JSON.parseObject(configuration.toMap().get(Constants.SYSPROP_GROOTSTREAM_CONFIG), CommonConfig.class);
            startTask();
        }
        return instance;
    }

    public static void  updateKnowledgeBase(KnowledgeBaseEntity knowledgeBaseEntity)  {

        try {
            knowledgeBaseEntity.getAbstractKnowledgeBase().updateKnowledgeBase(knowledgeBaseEntity.getKnowledgeConfig());
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }



    public static synchronized  void initKnowledgeBase(String name,AbstractKnowledgeBase abstractKnowledgeBase,RuntimeContext runtimeContext)   {

      if (instance == null) {
            getInstance(runtimeContext);
        }

            if (!KnowLedgeBaseEntityMap.containsKey(name)) {
                List<KnowLedgeFileEntity> knowLedgeFileEntityList = new ArrayList<>();
                KnowledgeBaseEntity knowledgeBaseEntity = new KnowledgeBaseEntity();
                knowledgeBaseEntity.setAbstractKnowledgeBase(abstractKnowledgeBase);
                knowledgeBaseEntity.setKnowLedgeFileEntityList(knowLedgeFileEntityList);

                try {
                    for(KnowledgeBaseConfig knowledgeConfig : engineConfig.getKnowledgeBaseConfig()){
                        if(name.equals(knowledgeConfig.getName())){
                            knowledgeBaseEntity.setKnowledgeConfig(knowledgeConfig);
                            break;
                        }
                    }
                    KnowLedgeBaseEntityMap.put(name, knowledgeBaseEntity);
                    for (String id : knowledgeBaseEntity.getKnowledgeConfig().getFiles()) {
                      //  KnowLedgeEntity knowLedgeEntity = getMetadata(id);
                        KnowLedgeFileEntity knowLedgeFileEntity1 =  new KnowLedgeFileEntity();
                        knowLedgeFileEntity1.setIsValid(1);
                        knowLedgeFileEntity1.setSha256("2a2defd9c96ea5434ac628d3dc4fc6790e30d84c218b02fe798dbc6772dbf76c");
                        knowLedgeFileEntity1.setPath(id);

/*
                        KnowLedgeEntity knowLedgeEntity2 =  new KnowLedgeEntity();
                        knowLedgeEntity2.setIsValid(1);
                        knowLedgeEntity2.setSha256("044228128a1cc7ae0f8d4bae9fdc73eac9dea3e074e7c15796d29167b89cfdee");
                        knowLedgeEntity2.setPath("http://192.168.44.12:9098/hos/knowledge_base_bucket/63220f3e-7792-43a8-9921-d01da16b76b2-aXBfYnVpbHRpbg==.mmdb");
*/

                        knowLedgeFileEntityList.add(knowLedgeFileEntity1);
                    }


                    updateKnowledgeBase(knowledgeBaseEntity);
                } catch (Exception e) {
                    logger.error("initKnowledgeBase  " + e.getMessage());
                }
            }


    }
    private static KnowLedgeFileEntity getMetadata(String id) {

        try{
            HttpClientPoolUtil httpClientPoolUtil = HttpClientPoolUtil.getInstance();
            String metadate  = httpClientPoolUtil.httpGet(URI.create("/v1/knowledge_base/"+ id +"/meta")) ;
            List<KnowLedgeFileEntity> knowLedgeFileEntityList = JSON.parseObject(metadate, new TypeReference<List<KnowLedgeFileEntity>>() {});
            return knowLedgeFileEntityList.get(0);

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


    public static synchronized  Boolean  checkUpdateMesssage (KnowledgeBaseEntity knowledgeBaseEntity)  {

       /* if(knowledgeBaseEntity.getKnowLedgeEntityList()!=null) {
            for (KnowLedgeEntity knowLedgeEntity : knowledgeBaseEntity.getKnowLedgeEntityList()) {

                KnowLedgeEntity metadata = getMetadata(knowLedgeEntity.getId());
                if (metadata != null) {
                    if (!knowLedgeEntity.getSha256().equals(metadata.getSha256())) {
                        return true;
                    }
                } else {
                    logger.error("get file Metadata " + knowLedgeEntity.getId() + " error ");
                }
            }
        }
        return false;*/

        return true;
    }

    public static void destroyKnowledgeBase() {
        stopTask();
    }

    @Override
    public void execute(JobExecutionContext jobExecutionContext) {

        if (!KnowLedgeBaseEntityMap.isEmpty()) {
            for (Map.Entry<String, KnowledgeBaseEntity> entry : KnowLedgeBaseEntityMap.entrySet()) {
                Boolean flag = null;
                try {
                    flag = checkUpdateMesssage(entry.getValue());
                    if (flag) {
                        updateKnowledgeBase(entry.getValue());
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage());
                }
            }
        }
    }



}
