package com.geedgenetworks.spi.processor;

public interface ProcessorFactory {
    String type();
    Processor<?> createProcessor();

}
