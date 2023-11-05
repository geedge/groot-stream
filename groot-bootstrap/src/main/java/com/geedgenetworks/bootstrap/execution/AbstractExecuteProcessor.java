package com.geedgenetworks.bootstrap.execution;

import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.Map;

public abstract  class AbstractExecuteProcessor<K, V>
        implements ExecuteProcessor<SingleOutputStreamOperator, JobRuntimeEnvironment> {
    protected  JobRuntimeEnvironment jobRuntimeEnvironment;
    protected final Map<String, Object> operatorConfigMap;

    protected final Map<K,V > operatorMap;

    protected AbstractExecuteProcessor(Map<String, Object> operatorConfigMap) {
        this.operatorConfigMap = operatorConfigMap;
        this.operatorMap = initialize(operatorConfigMap);
    }

    @Override
    public void setRuntimeEnvironment(JobRuntimeEnvironment jobRuntimeEnvironment) {
        this.jobRuntimeEnvironment = jobRuntimeEnvironment;

    }

    protected abstract Map<K, V> initialize(Map<String, Object> operatorConfigMap);
}
