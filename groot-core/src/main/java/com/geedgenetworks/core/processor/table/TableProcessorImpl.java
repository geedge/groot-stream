package com.geedgenetworks.core.processor.table;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.core.pojo.TableConfig;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;

import java.util.Map;


public class TableProcessorImpl implements TableProcessor {

    @Override
    public DataStream<Event> processorFunction(DataStream<Event> grootEventDataStream, TableConfig tableConfig, ExecutionConfig config) throws Exception {
        if (tableConfig.getParallelism() != 0) {
            return grootEventDataStream
                    .flatMap(new TableProcessorFunction(tableConfig))
                    .setParallelism(tableConfig.getParallelism())
                    .name(tableConfig.getName());
        } else {
            return grootEventDataStream
                    .flatMap(new TableProcessorFunction(tableConfig))
                    .name(tableConfig.getName());
        }
    }

    @Override
    public String type() {
        return "table";
    }
}
