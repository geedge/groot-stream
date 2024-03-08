package com.geedgenetworks.core.udf.knowlegdebase;

import com.geedgenetworks.common.config.KnowledgeBaseConfig;
import com.geedgenetworks.core.pojo.KnowledgeBaseConfigWithHandler;
import com.geedgenetworks.core.udf.knowlegdebase.handler.AbstractKnowledgeBaseHandler;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.quartz.Job;
import org.quartz.JobExecutionContext;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class KnowledgeBaseUpdateJob implements Job {


    @Getter
    private static final Map<String, KnowledgeBaseConfigWithHandler> KnowLedgeBaseEntityMap= new ConcurrentHashMap<>() ;


    public static void  updateKnowledgeBase(KnowledgeBaseConfigWithHandler knowledgeBaseConfigWithHandler)  {

        try {
            knowledgeBaseConfigWithHandler.getAbstractKnowledgeBaseHandler().updateKnowledgeBase();
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }



    public static synchronized  boolean registerKnowledgeBase(AbstractKnowledgeBaseHandler abstractKnowledgeBaseHandler, KnowledgeBaseConfig knowledgeBaseConfig)   {


            boolean isSuccess = false;
            if (!KnowLedgeBaseEntityMap.containsKey(knowledgeBaseConfig.getName())) {
                KnowledgeBaseConfigWithHandler knowledgeBaseConfigWithHandler = new KnowledgeBaseConfigWithHandler();
                knowledgeBaseConfigWithHandler.setAbstractKnowledgeBaseHandler(abstractKnowledgeBaseHandler);
                knowledgeBaseConfigWithHandler.setKnowledgeConfig(knowledgeBaseConfig);
                try {
                    isSuccess = abstractKnowledgeBaseHandler.initKnowledgeBase(knowledgeBaseConfig);
                    KnowLedgeBaseEntityMap.put(knowledgeBaseConfig.getName(), knowledgeBaseConfigWithHandler);
                    log.warn("initKnowledgeBase "+knowledgeBaseConfig.getName() + "Success");
                } catch (Exception e) {
                    log.error("initKnowledgeBase "+knowledgeBaseConfig.getName() + "Error" + e.getMessage());
                }
            } else {
                isSuccess = true;
            }
            return isSuccess;

    }

    public static synchronized  void removeKnowledgeBase(String kbName)   {

        KnowLedgeBaseEntityMap.remove(kbName);
    }
    @Override
    public void execute(JobExecutionContext jobExecutionContext) {

        log.warn("Start scheduler knowledge_base update job !");
        if (!KnowLedgeBaseEntityMap.isEmpty()) {
            for (Map.Entry<String, KnowledgeBaseConfigWithHandler> entry : KnowLedgeBaseEntityMap.entrySet()) {
                try {
                    if("http".equals(entry.getValue().getKnowledgeConfig().getFsType())) {
                        updateKnowledgeBase(entry.getValue());
                    }
                } catch (Exception e) {
                    log.error(e.getMessage());
                }
            }
        }
    }



}
