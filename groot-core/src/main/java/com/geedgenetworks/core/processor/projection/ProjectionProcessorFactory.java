package com.geedgenetworks.core.processor.projection;

import com.geedgenetworks.api.processor.Processor;
import com.geedgenetworks.api.factory.ProcessorFactory;

public class ProjectionProcessorFactory implements ProcessorFactory {

    @Override
    public Processor<?> createProcessor() {
        return new ProjectionProcessor();
    }

    @Override
    public String type() {
        return "projection";
    }

}
