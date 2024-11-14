package com.geedgenetworks.core.processor.projection;

import com.geedgenetworks.api.processor.Processor;
import com.geedgenetworks.api.factory.ProcessorFactory;

public class ProjectionProcessorFactory implements ProcessorFactory {
    private static final String IDENTIFIER = "projection";
    @Override
    public Processor<?> createProcessor() {
        return new ProjectionProcessor();
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
