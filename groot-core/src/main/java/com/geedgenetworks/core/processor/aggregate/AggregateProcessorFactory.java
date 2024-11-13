package com.geedgenetworks.core.processor.aggregate;

import com.geedgenetworks.api.processor.Processor;
import com.geedgenetworks.api.factory.ProcessorFactory;
import org.apache.flink.configuration.ConfigOption;

import java.util.Set;

public class AggregateProcessorFactory implements ProcessorFactory {

    @Override
    public String type() {
        return "aggregate";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Set.of();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Set.of();
    }

    @Override
    public Processor<?> createProcessor() {
        return new AggregateProcessor();
    }
}
