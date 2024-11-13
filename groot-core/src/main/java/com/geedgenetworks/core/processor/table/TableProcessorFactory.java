package com.geedgenetworks.core.processor.table;

import com.geedgenetworks.api.processor.Processor;
import com.geedgenetworks.api.factory.ProcessorFactory;

public class TableProcessorFactory implements ProcessorFactory {

    @Override
    public Processor<?> createProcessor() {
        return new TableProcessor();
    }

    @Override
    public String type() {
        return "table";
    }

}
