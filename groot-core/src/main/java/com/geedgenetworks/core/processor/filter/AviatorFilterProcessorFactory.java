package com.geedgenetworks.core.processor.filter;

import com.geedgenetworks.api.processor.Processor;
import com.geedgenetworks.api.factory.ProcessorFactory;

public class AviatorFilterProcessorFactory implements ProcessorFactory {

    @Override
    public Processor<?> createProcessor() {
        return new AviatorFilterProcessor();
    }

    @Override
    public String type() {
        return "aviator";
    }

}
