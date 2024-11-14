package com.geedgenetworks.core.processor.table;

import com.geedgenetworks.api.processor.Processor;
import com.geedgenetworks.api.factory.ProcessorFactory;

public class TableProcessorFactory implements ProcessorFactory {
    private static final String IDENTIFIER = "table";
    @Override
    public Processor<?> createProcessor() {
        return new TableProcessor();
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
