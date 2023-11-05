package com.geedgenetworks.bootstrap.execution;

import com.geedgenetworks.bootstrap.exception.JobExecuteException;

public interface ExecuteProcessor<T, ENV extends RuntimeEnvironment> {

    T execute(T inputDataStream, Edge edge) throws JobExecuteException;

    void setRuntimeEnvironment(ENV runtimeEnvironment);

}
