package com.geedgenetworks.bootstrap.execution;

import com.geedgenetworks.common.utils.ReflectionUtils;
import com.geedgenetworks.core.filter.Filter;
import com.geedgenetworks.core.processor.aggregate.AggregateProcessor;
import com.geedgenetworks.core.processor.projection.ProjectionProcessor;
import com.typesafe.config.Config;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.function.BiConsumer;

public abstract  class AbstractExecutor<K, V>
        implements Executor<SingleOutputStreamOperator, JobRuntimeEnvironment> {
    protected  JobRuntimeEnvironment jobRuntimeEnvironment;
    protected final Config operatorConfig;
    protected final Map<K,V> operatorMap;
    protected final Map<String,Filter> filterMap = new HashMap<>();
    protected final Map<String,ProjectionProcessor> projectionProcessorMap = new HashMap<>();
    protected final Map<String, AggregateProcessor> aggregateProcessorMap = new HashMap<>();

    protected AbstractExecutor(List<URL> jarPaths, Config operatorConfig) {
        this.operatorConfig = operatorConfig;
        this.operatorMap = initialize(jarPaths, operatorConfig);
        ServiceLoader<Filter> filters = ServiceLoader.load(Filter.class);
        for (Filter filter : filters) {
            this.filterMap.put(filter.type(), filter);
        }
        ServiceLoader<ProjectionProcessor> projectionProcessors = ServiceLoader.load(ProjectionProcessor.class);
        for (ProjectionProcessor projectionProcessor : projectionProcessors) {
            this.projectionProcessorMap.put(projectionProcessor.type(), projectionProcessor);
        }
        ServiceLoader<AggregateProcessor> aggregateProcessors = ServiceLoader.load(AggregateProcessor.class);
        for (AggregateProcessor aggregateProcessor : aggregateProcessors) {
            this.aggregateProcessorMap.put(aggregateProcessor.type(), aggregateProcessor);
        }
    }

    @Override
    public void setRuntimeEnvironment(JobRuntimeEnvironment jobRuntimeEnvironment) {
        this.jobRuntimeEnvironment = jobRuntimeEnvironment;

    }

    protected abstract Map<K, V> initialize(List<URL> jarPaths, Config operatorConfig);

    protected  static final BiConsumer<ClassLoader, URL> ADD_URL_TO_CLASSLOADER =
            (classLoader, url) -> {
                if (classLoader.getClass().getName().endsWith("SafetyNetWrapperClassLoader")) {
                    URLClassLoader c =
                            (URLClassLoader) ReflectionUtils.getField(classLoader, "inner").get();
                    ReflectionUtils.invoke(c, "addURL", url);
                } else if (classLoader instanceof URLClassLoader) {
                    ReflectionUtils.invoke(classLoader, "addURL", url);
                } else {
                    throw new RuntimeException(
                            "Unsupported classloader: " + classLoader.getClass().getName());
                }
            };


}
