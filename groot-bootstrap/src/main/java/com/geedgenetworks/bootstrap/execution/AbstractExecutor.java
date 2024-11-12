package com.geedgenetworks.bootstrap.execution;

import com.geedgenetworks.common.utils.ReflectionUtils;
import com.geedgenetworks.api.connector.event.Event;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.function.BiConsumer;

public abstract class AbstractExecutor<E, C> implements Executor<DataStream<Event>> {
    public E environment;
    protected final C jobConfig;

    protected AbstractExecutor(E environment, C jobConfig) {
        this.environment = environment;
        this.jobConfig = jobConfig;
        initialize(jobConfig);
    }
    protected abstract void initialize(C jobConfig);

    protected static final BiConsumer<ClassLoader, URL> ADD_URL_TO_CLASSLOADER =
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
