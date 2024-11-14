package com.geedgenetworks.core.processor.filter;

import com.geedgenetworks.api.processor.Processor;
import com.geedgenetworks.api.factory.ProcessorFactory;

public class FilterProcessorFactory implements ProcessorFactory {
    private static final String IDENTIFIER = "filter";
    private static final String LEGACY_IDENTIFIER = "aviator";

    @Override
    public Processor<?> createProcessor() {
        return new FilterProcessor();
    }

    @Override
    public String type() {
        return IDENTIFIER;
    }

    @Override
    public boolean supportsType(String type) {
        return LEGACY_IDENTIFIER.equalsIgnoreCase(type) || IDENTIFIER.equalsIgnoreCase(type);
    }

}
