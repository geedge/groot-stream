package com.geedgenetworks.bootstrap.execution;

import com.geedgenetworks.bootstrap.exception.JobExecuteException;

public interface Executor<T, ENV extends RuntimeEnvironment> {

    T execute(T dataStream, OperatorNode edge) throws JobExecuteException;

    void setRuntimeEnvironment(ENV runtimeEnvironment);

}
