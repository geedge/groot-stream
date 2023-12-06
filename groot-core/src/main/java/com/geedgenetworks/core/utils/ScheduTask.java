package com.geedgenetworks.core.utils;

import com.geedgenetworks.common.config.KnowledgeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import static com.geedgenetworks.core.utils.KnowledgeBaseSchedule.*;

public class ScheduTask extends TimerTask {

    private static Timer timer;
    private static final Logger logger = LoggerFactory.getLogger(ScheduTask.class);

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
        //System.out.println("Static task is running...");
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
