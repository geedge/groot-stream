package com.geedgenetworks.core.utils.KnowlegdeBase;

import com.geedgenetworks.core.utils.SchedulerUtils;
import org.quartz.*;

public class KnowledgeBaseScheduler {

    public static void startTask() {
        try {
            // 创建调度器
            Scheduler scheduler = SchedulerUtils.getScheduler();
            // 创建定时任务
            JobDetail jobDetail = JobBuilder.newJob(KnowledgeBaseUpdateJob.class)
                    .withIdentity("knowledgeBaseUpdateJob", "knowledgeBase")
                    .build();

            // 创建触发器，设置触发时间
            Trigger trigger = TriggerBuilder.newTrigger()
                    .withIdentity("knowledgeBaseUpdateTrigger", "knowledgeBase")
                    .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                            .withIntervalInMinutes(5)  // 每隔5min触发一次
                            .repeatForever())
                    .build();

            // 将定时任务和触发器绑定到调度器
            scheduler.scheduleJob(jobDetail, trigger);
            // 启动调度器
            scheduler.start();

        } catch (SchedulerException e) {
            throw new RuntimeException();
        }
    }

    public static void stopTask() {
        try {
            // 创建调度器
            Scheduler scheduler = SchedulerUtils.getScheduler();
            // 启动调度器
            scheduler.shutdown();
        } catch (SchedulerException e) {
            throw new RuntimeException();
        }
    }
}
