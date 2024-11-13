package com.geedgenetworks.core.processor.projection;

import com.geedgenetworks.api.processor.Processor;
import com.geedgenetworks.api.factory.ProcessorFactory;
import org.apache.flink.configuration.ConfigOption;

import java.util.Set;

public class ProjectionProcessorFactory implements ProcessorFactory {

    @Override
    public Processor<?> createProcessor() {
        return new ProjectionProcessor();
    }

    @Override
    public String type() {
        return "projection";
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
