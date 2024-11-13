package com.geedgenetworks.core.processor.split;

import com.geedgenetworks.api.processor.Processor;
import com.geedgenetworks.api.factory.ProcessorFactory;
import org.apache.flink.configuration.ConfigOption;

import java.util.Set;

public class SplitProcessorFactory implements ProcessorFactory {

    @Override
    public Processor<?> createProcessor() {
        return new SplitProcessor();
    }

    @Override
    public String type() {
        return "split";
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
