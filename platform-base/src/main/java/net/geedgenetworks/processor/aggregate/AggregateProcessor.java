package net.geedgenetworks.processor.aggregate;

import net.geedgenetworks.pojo.Aggregate;
import net.geedgenetworks.pojo.GrootEvent;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public interface AggregateProcessor {

    SingleOutputStreamOperator<GrootEvent> aggregateProcessorFunction(SingleOutputStreamOperator<GrootEvent> singleOutputStreamOperator, Aggregate aggregate) throws Exception;
}
