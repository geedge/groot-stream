package com.geedgenetworks.bootstrap.execution;

import com.geedgenetworks.common.utils.ReflectionUtils;
import com.geedgenetworks.spi.table.event.Event;
import com.typesafe.config.Config;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.function.BiConsumer;

public abstract class AbstractExecutor<K, V>
        implements Executor<DataStream<Event>, JobRuntimeEnvironment> {
    protected  JobRuntimeEnvironment jobRuntimeEnvironment;
    protected final Config operatorConfig;
    protected final Map<K,V> operatorMap;

    protected AbstractExecutor(List<URL> jarPaths, Config operatorConfig) {
        this.operatorConfig = operatorConfig;
        this.operatorMap = initialize(jarPaths, operatorConfig);
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
