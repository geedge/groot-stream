package com.geedgenetworks.core.utils;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;

public class SchedulerUtils {
    private static Scheduler scheduler;

    /**
     * 获取调度器实例
     *
     * @return 调度器实例
     * @throws SchedulerException 如果创建调度器失败
     */
    public static Scheduler getScheduler() throws SchedulerException {
        if (scheduler == null) {
            synchronized (SchedulerUtils.class) {
                if (scheduler == null) {
                    scheduler = createScheduler();
                }
            }
        }
        return scheduler;
    }

    /**
     * 创建调度器实例
     *
     * @return 调度器实例
     * @throws SchedulerException 如果创建调度器失败
     */
    private static Scheduler createScheduler() throws SchedulerException {
        StdSchedulerFactory factory = new StdSchedulerFactory();
        return factory.getScheduler();
    }

    /**
     * 启动调度器
     *
     * @throws SchedulerException 如果启动调度器失败
     */
    public static void startScheduler() throws SchedulerException {
        Scheduler scheduler = getScheduler();
        if (!scheduler.isStarted()) {
            scheduler.start();
        }
    }

    /**
     * 关闭调度器
     *
     * @throws SchedulerException 如果关闭调度器失败
     */
    public static void shutdownScheduler() throws SchedulerException {
        Scheduler scheduler = getScheduler();
        if (!scheduler.isShutdown()) {
            scheduler.shutdown();
        }
    }
}
