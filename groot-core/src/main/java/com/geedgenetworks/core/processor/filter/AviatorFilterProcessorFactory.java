package com.geedgenetworks.core.processor.filter;

import com.geedgenetworks.api.processor.Processor;
import com.geedgenetworks.api.factory.ProcessorFactory;
import org.apache.flink.configuration.ConfigOption;

import java.util.Set;

public class AviatorFilterProcessorFactory implements ProcessorFactory {

    @Override
    public Processor<?> createProcessor() {
        return new AviatorFilterProcessor();
    }

    @Override
    public String type() {
        return "aviator";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Set.of();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Set.of();
    }
}
