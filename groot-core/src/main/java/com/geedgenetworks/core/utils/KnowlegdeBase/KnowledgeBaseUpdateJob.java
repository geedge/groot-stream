package com.geedgenetworks.core.utils.KnowlegdeBase;

import com.esotericsoftware.minlog.Log;
import com.geedgenetworks.common.config.KnowledgeBaseConfig;
import com.geedgenetworks.core.pojo.KnowledgeBaseConfigWithHandler;
import com.geedgenetworks.core.utils.KnowlegdeBase.Handler.AbstractKnowledgeBaseHandler;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            Log.error(e.getMessage());
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
                    Log.warn("initKnowledgeBase "+knowledgeBaseConfig.getName() + "Success");
                } catch (Exception e) {
                    Log.error("initKnowledgeBase "+knowledgeBaseConfig.getName() + "Error" + e.getMessage());
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

        Log.warn("Start scheduler knowledge_base update job !");
        if (!KnowLedgeBaseEntityMap.isEmpty()) {
            for (Map.Entry<String, KnowledgeBaseConfigWithHandler> entry : KnowLedgeBaseEntityMap.entrySet()) {
                try {
                    updateKnowledgeBase(entry.getValue());
                } catch (Exception e) {
                    Log.error(e.getMessage());
                }
            }
        }
    }



}
