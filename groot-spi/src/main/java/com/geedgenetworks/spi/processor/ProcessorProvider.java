package com.geedgenetworks.spi.processor;

import java.util.ServiceLoader;

public class ProcessorProvider {

    public static Processor<?> load(String type) {
        ServiceLoader<ProcessorFactory> loader = ServiceLoader.load(ProcessorFactory.class);
        for (ProcessorFactory factory : loader) {
            if (factory.type().equals(type)) {
                return factory.createProcessor();
            }
        }
        throw new IllegalArgumentException("Processor type not found: " + type);
    }

}
