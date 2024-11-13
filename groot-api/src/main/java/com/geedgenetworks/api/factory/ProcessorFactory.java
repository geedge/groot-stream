package com.geedgenetworks.api.factory;

import com.geedgenetworks.api.processor.Processor;

public interface ProcessorFactory extends Factory {
    Processor<?> createProcessor();
}
