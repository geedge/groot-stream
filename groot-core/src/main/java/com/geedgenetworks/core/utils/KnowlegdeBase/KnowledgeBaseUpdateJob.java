package com.geedgenetworks.core.utils.KnowlegdeBase;

import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.config.CommonConfig;
import com.geedgenetworks.common.config.KnowledgeBaseConfig;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.core.pojo.KnowLedgeFileEntity;
import com.geedgenetworks.core.pojo.KnowledgeBaseEntity;
import lombok.Getter;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.geedgenetworks.core.utils.KnowlegdeBase.AbstractKnowledgeBase.getMetadata;
import static com.geedgenetworks.core.utils.KnowlegdeBase.KnowledgeBaseScheduler.startTask;
import static com.geedgenetworks.core.utils.KnowlegdeBase.KnowledgeBaseScheduler.stopTask;


public class KnowledgeBaseUpdateJob implements Job {


    @Getter
    private static final Map<String, KnowledgeBaseEntity> KnowLedgeBaseEntityMap= new ConcurrentHashMap<>() ;
    private static final Logger logger = LoggerFactory.getLogger(KnowledgeBaseUpdateJob.class);

    private static KnowledgeBaseUpdateJob instance;


    public static synchronized KnowledgeBaseUpdateJob getInstance()  {
        if (instance == null) {
            instance = new KnowledgeBaseUpdateJob();
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



    public static synchronized  void initKnowledgeBase(String name,AbstractKnowledgeBase abstractKnowledgeBase,CommonConfig commonConfig)   {


        if (instance == null) {
            getInstance();
        }

            if (!KnowLedgeBaseEntityMap.containsKey(name)) {
                List<KnowLedgeFileEntity> knowLedgeFileEntityList = new ArrayList<>();
                KnowledgeBaseEntity knowledgeBaseEntity = new KnowledgeBaseEntity();
                knowledgeBaseEntity.setAbstractKnowledgeBase(abstractKnowledgeBase);
                knowledgeBaseEntity.setKnowLedgeFileEntityList(knowLedgeFileEntityList);

                try {
                    for(KnowledgeBaseConfig knowledgeBaseConfig : commonConfig.getKnowledgeBaseConfig()){
                        if(!knowledgeBaseConfig.getProperties().containsKey("fs_type") || !knowledgeBaseConfig.getProperties().containsKey("fs_default_path")){
                            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "initKnowledgeBase error! the knowledge_base properties should contain fs_type and fs_default_path .");
                        }
                        if(name.equals(knowledgeBaseConfig.getName())){
                            knowledgeBaseEntity.setKnowledgeConfig(knowledgeBaseConfig);
                            break;
                        }
                    }
                    KnowLedgeBaseEntityMap.put(name, knowledgeBaseEntity);
                    for (String id : knowledgeBaseEntity.getKnowledgeConfig().getFiles()) {

                        KnowLedgeFileEntity knowLedgeEntity = getMetadata(knowledgeBaseEntity.getKnowledgeConfig().getProperties().get("fs_type"), knowledgeBaseEntity.getKnowledgeConfig().getProperties().get("fs_default_path"),id);
                        if(knowLedgeEntity!=null){
                            knowLedgeFileEntityList.add(knowLedgeEntity);
                        }
                    }

                    if(!knowLedgeFileEntityList.isEmpty()) {
                        updateKnowledgeBase(knowledgeBaseEntity);
                    }
                } catch (Exception e) {
                    logger.error("initKnowledgeBase  " + e.getMessage());
                }
            }


    }


    public static synchronized  Boolean ifNeedUpdate(KnowledgeBaseEntity knowledgeBaseEntity)  {

        if(knowledgeBaseEntity.getKnowLedgeFileEntityList()!=null) {
            for (KnowLedgeFileEntity knowLedgeEntity : knowledgeBaseEntity.getKnowLedgeFileEntityList()) {

                KnowLedgeFileEntity metadata = getMetadata(knowledgeBaseEntity.getKnowledgeConfig().getProperties().get("fs_type"),knowledgeBaseEntity.getKnowledgeConfig().getProperties().get("fs_default_path"),knowLedgeEntity.getKb_id());
                if (metadata != null) {
                    if (!knowLedgeEntity.getSha256().equals(metadata.getSha256())) {
                        return true;
                    }
                } else {
                    logger.error("get file Metadata " + knowLedgeEntity.getKb_id()+ " error ");
                }
            }
        }
        return false;

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
                    flag = ifNeedUpdate(entry.getValue());
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
