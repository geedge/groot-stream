package com.geedgenetworks.core.processor.aggregate;

import com.geedgenetworks.api.processor.Processor;
import com.geedgenetworks.api.factory.ProcessorFactory;

public class AggregateProcessorFactory implements ProcessorFactory {

    @Override
    public String type() {
        return "aggregate";
    }

    @Override
    public Processor<?> createProcessor() {
        return new AggregateProcessor();
    }
}
