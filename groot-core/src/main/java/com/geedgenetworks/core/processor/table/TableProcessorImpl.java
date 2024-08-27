package com.geedgenetworks.core.processor.table;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.core.pojo.TableConfig;
import com.geedgenetworks.core.processor.projection.ProjectionProcessFunction;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.time.Duration;

public class TableProcessorImpl implements TableProcessor {

    @Override
    public SingleOutputStreamOperator<Event> processorFunction(SingleOutputStreamOperator<Event> grootEventSingleOutputStreamOperator, TableConfig tableConfig, ExecutionConfig config) throws Exception {

        if (tableConfig.getParallelism() != 0) {
            return grootEventSingleOutputStreamOperator
                    .flatMap(new TableProcessorFunction(tableConfig))
                    .setParallelism(tableConfig.getParallelism())
                    .name(tableConfig.getName());
        } else {
            return grootEventSingleOutputStreamOperator
                    .flatMap(new TableProcessorFunction(tableConfig))
                    .name(tableConfig.getName());
        }
    }

    @Override
    public String type() {
        return "table";
    }
}
