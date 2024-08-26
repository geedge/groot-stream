package com.geedgenetworks.bootstrap.execution;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.utils.ReflectionUtils;
import com.geedgenetworks.core.filter.Filter;
import com.geedgenetworks.core.processor.Processor;
import com.geedgenetworks.core.split.Split;
import com.typesafe.config.Config;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.function.BiConsumer;

public abstract  class AbstractExecutor<K, V>
        implements Executor<DataStream<Event>, JobRuntimeEnvironment> {
    protected  JobRuntimeEnvironment jobRuntimeEnvironment;
    protected final Config operatorConfig;
    protected final Map<K,V> operatorMap;
    protected final Map<String,Filter> filterMap = new HashMap<>();
    protected final Map<String, Split> splitMap = new HashMap<>();
    protected final Map<String, Processor> processorMap = new HashMap<>();

    protected AbstractExecutor(List<URL> jarPaths, Config operatorConfig) {
        this.operatorConfig = operatorConfig;
        this.operatorMap = initialize(jarPaths, operatorConfig);
        ServiceLoader<Filter> filters = ServiceLoader.load(Filter.class);
        for (Filter filter : filters) {
            this.filterMap.put(filter.type(), filter);
        }
        ServiceLoader<Split> splits = ServiceLoader.load(Split.class);
        for (Split split : splits) {
            this.splitMap.put(split.type(), split);
        }
        ServiceLoader<Processor> processors = ServiceLoader.load(Processor.class);
        for (Processor processor : processors) {
            this.processorMap.put(processor.type(), processor);
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
