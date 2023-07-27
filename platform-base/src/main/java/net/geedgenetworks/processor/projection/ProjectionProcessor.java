package net.geedgenetworks.processor.projection;

import net.geedgenetworks.pojo.GrootEvent;
import net.geedgenetworks.pojo.Projection;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public interface ProjectionProcessor {

    SingleOutputStreamOperator<GrootEvent> projectionProcessorFunction(SingleOutputStreamOperator<GrootEvent> grootEventSingleOutputStreamOperator, Projection projection) throws Exception;
}
