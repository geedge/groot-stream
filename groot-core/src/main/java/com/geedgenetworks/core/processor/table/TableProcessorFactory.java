package com.geedgenetworks.core.processor.table;
import com.geedgenetworks.api.processor.Processor;
import com.geedgenetworks.api.factory.ProcessorFactory;
import org.apache.flink.configuration.ConfigOption;

import java.util.Set;

public class TableProcessorFactory implements ProcessorFactory {

    @Override
    public Processor<?> createProcessor() {
        return new TableProcessor();
    }

    @Override
    public String type() {
        return "table";
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
