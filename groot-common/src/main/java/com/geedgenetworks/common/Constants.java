package com.geedgenetworks.common;

public final class Constants {

    public static final String DEFAULT_JOB_NAME="groot-stream-job";
    public static final String SOURCES = "sources";
    public static final String FILTERS = "filters";
    public static final String PREPROCESSING_PIPELINES = "preprocessing_pipelines";
    public static final String PROCESSING_PIPELINES = "processing_pipelines";
    public static final String POSTPROCESSING_PIPELINES = "postprocessing_pipelines";
    public static final String SINKS = "sinks";
    public static final int SYSTEM_EXIT_CODE = 2618;
    public static final String APPLICATION = "application";

    public static final String APPLICATION_ENV ="env";
    public static final String APPLICATION_TOPOLOGY = "topology";
    public static final String JOB_NAME = "name";
    public static final String GROOT_LOGO = "\n" +
            "   ____                     _         ____   _                                \n" +
            "  / ___| _ __  ___    ___  | |_      / ___| | |_  _ __  ___   __ _  _ __ ___  \n" +
            " | |  _ | '__|/ _ \\  / _ \\ | __|_____\\___ \\ | __|| '__|/ _ \\ / _` || '_ ` _ \\ \n" +
            " | |_| || |  | (_) || (_) || |_|_____|___) || |_ | |  |  __/| (_| || | | | | |\n" +
            "  \\____||_|   \\___/  \\___/  \\__|     |____/  \\__||_|   \\___| \\__,_||_| |_| |_|\n" +
            "                                                                              \n";


    public static final String SYSPROP_GROOTSTREAM_CONFIG = "grootstream.config";
    public static final String SYSPROP_GROOTSTREAM_PREFIX = "props.";
    public static final String SYSPROP_GROOTSTREAM_HADOOP_PREFIX = "hadoop.";
    public static final String SYSPROP_GROOTSTREAM_KB_SCHEDULER_INTERVAL_NAME = "scheduler.knowledge_base.update.interval.minutes";

    public static final String HAZELCAST_GROOTSTREAM_CONFIG_FILE_PREFIX = "grootstream";
    public static final String HAZELCAST_GROOTSTREAM_CONFIG_DEFAULT = "grootstream.yaml";

    /**
     * The default port number for the cluster auto-discovery mechanism's multicast communication.
     */
    public static final int DEFAULT_GROOTSTREAM_MULTICAST_PORT = 53326;


    public static final String GROOTSTREAM_CLUSTER_NAME = "gt:impl:grootStreamServer";

    public static final String SYSPROP_UDF_PLUGIN_CONFIG = "udf.config";
    public static final String HAZELCAST_UDF_PLUGIN_CONFIG_FILE_PREFIX = "udf";
    public static final String HAZELCAST_UDF_PLUGIN_CONFIG_DEFAULT = "udf.plugins";








}
