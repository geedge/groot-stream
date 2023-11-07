package com.geedgenetworks.bootstrap.execution;

import com.typesafe.config.Config;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.Map;

public abstract  class AbstractExecuteProcessor<K, V>
        implements ExecuteProcessor<SingleOutputStreamOperator, JobRuntimeEnvironment> {
    protected  JobRuntimeEnvironment jobRuntimeEnvironment;

    protected final Config operatorConfig;

    protected final Map<K,V> operatorMap;

    protected AbstractExecuteProcessor(Config  operatorConfig) {
        this.operatorConfig = operatorConfig;
        this.operatorMap = initialize(operatorConfig);
    }

    @Override
    public void setRuntimeEnvironment(JobRuntimeEnvironment jobRuntimeEnvironment) {
        this.jobRuntimeEnvironment = jobRuntimeEnvironment;

    }

    protected abstract Map<K, V> initialize(Config  operatorConfig);
}
