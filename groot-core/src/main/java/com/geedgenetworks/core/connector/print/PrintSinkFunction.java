package com.geedgenetworks.core.connector.print;

import com.geedgenetworks.api.connector.serialization.EncodingFormat;
import com.geedgenetworks.api.connector.event.Event;
import com.geedgenetworks.api.connector.type.StructType;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

import static com.geedgenetworks.core.connector.print.PrintMode.*;
public class PrintSinkFunction extends RichSinkFunction<Event> {
    private static final Logger LOG = LoggerFactory.getLogger(PrintSinkFunction.class);
    private final StructType dataType;
    private final SerializationSchema<Event> serialization;
    private final PrintMode printMode;
    private transient Counter numRecordsIn;
    private transient Counter numRecordsOut;
    private transient Meter numRecordsInPerSecond;
    private transient Meter numRecordsOutPerSecond;

    public PrintSinkFunction(StructType dataType, EncodingFormat encodingFormat, PrintMode printMode) {
        this.dataType = dataType;
        this.serialization = encodingFormat.createRuntimeEncoder(dataType);
        this.printMode = printMode;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        serialization.open(null);
        initMetric();
    }

    private void initMetric() throws Exception {
        numRecordsIn = getRuntimeContext().getMetricGroup().counter("printNumRecordsIn");
        numRecordsOut = getRuntimeContext().getMetricGroup().counter("printNumRecordsOut");
        numRecordsInPerSecond = getRuntimeContext().getMetricGroup().meter("printNumRecordsInPerSecond", new MeterView(numRecordsIn));
        numRecordsOutPerSecond = getRuntimeContext().getMetricGroup().meter("printNumRecordsOutPerSecond", new MeterView(numRecordsOut));
    }

    @Override
    public void invoke(Event value, Context context) throws Exception {
        numRecordsIn.inc();
        byte[] bytes = serialization.serialize(value);
        String msg = new String(bytes, StandardCharsets.UTF_8);
        if(printMode == STDOUT){
            System.out.println(msg);
        } else if (printMode == LOG_INFO) {
            LOG.info(msg);
        } else if (printMode == LOG_WARN) {
            LOG.warn(msg);
        }
        numRecordsOut.inc();
    }
}
