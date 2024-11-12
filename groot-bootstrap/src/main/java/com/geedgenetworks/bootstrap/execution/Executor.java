package com.geedgenetworks.bootstrap.execution;

import com.geedgenetworks.bootstrap.exception.JobExecuteException;

public interface Executor<T> {

    T execute(T dataStream, JobTopologyNode jobTopologyNode) throws JobExecuteException;
}
