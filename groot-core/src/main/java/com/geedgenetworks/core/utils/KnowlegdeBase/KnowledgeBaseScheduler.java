package com.geedgenetworks.core.utils.KnowlegdeBase;

import com.esotericsoftware.minlog.Log;
import com.geedgenetworks.core.utils.SchedulerUtils;
import lombok.extern.slf4j.Slf4j;
import org.quartz.*;

@Slf4j
public class KnowledgeBaseScheduler {

    private final static String JOB_NAME = "knowledgeBaseUpdateJob";
    private final static String JOB_GROUP = "knowledgeBase";
    private final static String TRIGGER_NAME = "knowledgeBaseUpdateJob";
    private static boolean methodCalled = false;


    public synchronized static void startSchedulerForKnowledgeBase(int intervalInMinutes) throws SchedulerException {

        if(!methodCalled){

                // 创建调度器
                Scheduler scheduler = SchedulerUtils.getScheduler();
                // 创建定时任务
                JobDetail jobDetail = JobBuilder.newJob(KnowledgeBaseUpdateJob.class)
                        .withIdentity(JOB_NAME, JOB_GROUP)
                        .build();

                // 创建触发器，设置触发时间
                Trigger trigger = TriggerBuilder.newTrigger()
                        .withIdentity(TRIGGER_NAME, JOB_GROUP)
                        .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                                .withIntervalInMinutes(intervalInMinutes)  // 每隔多少分钟触发一次
                                .repeatForever())
                        .build();

                // 将定时任务和触发器绑定到调度器
                scheduler.scheduleJob(jobDetail, trigger);
                // 启动调度器
                SchedulerUtils.startScheduler();
                methodCalled =true;

        }
    }

    public static void stopSchedulerForKnowledgeBase() throws SchedulerException {

            // 创建调度器
            Scheduler scheduler = SchedulerUtils.getScheduler();

            if(scheduler.checkExists(JobKey.jobKey(JOB_NAME))){
                scheduler.deleteJob(JobKey.jobKey(JOB_NAME));
            }

            if(scheduler.getCurrentlyExecutingJobs().isEmpty()){
                SchedulerUtils.shutdownScheduler();
            }
            // 启动调度器
    }
}
