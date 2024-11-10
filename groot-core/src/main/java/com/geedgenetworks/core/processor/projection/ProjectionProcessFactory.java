package com.geedgenetworks.core.processor.projection;

import com.geedgenetworks.spi.processor.Processor;
import com.geedgenetworks.spi.processor.ProcessorFactory;

public class ProjectionProcessFactory implements ProcessorFactory {

    @Override
    public String type() {
        return "projection";
    }

    @Override
    public Processor<?> createProcessor() {
        return new ProjectionProcessor();
    }
}
