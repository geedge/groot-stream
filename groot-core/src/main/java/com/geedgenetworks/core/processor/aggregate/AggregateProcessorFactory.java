package com.geedgenetworks.core.processor.aggregate;

import com.geedgenetworks.api.processor.Processor;
import com.geedgenetworks.api.factory.ProcessorFactory;

public class AggregateProcessorFactory implements ProcessorFactory {

    private static final String IDENTIFIER = "aggregate";

    @Override
    public Processor<?> createProcessor() {
        return new AggregateProcessor();
    }

    @Override
    public String type() {
        return IDENTIFIER;
    }

    @Override
    public boolean supportsType(String type) {
        return IDENTIFIER.equalsIgnoreCase(type);
    }
}
