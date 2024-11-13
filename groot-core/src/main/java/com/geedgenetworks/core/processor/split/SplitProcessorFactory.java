package com.geedgenetworks.core.processor.split;

import com.geedgenetworks.api.processor.Processor;
import com.geedgenetworks.api.factory.ProcessorFactory;

public class SplitProcessorFactory implements ProcessorFactory {

    @Override
    public Processor<?> createProcessor() {
        return new SplitProcessor();
    }

    @Override
    public String type() {
        return "split";
    }

}
