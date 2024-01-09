package com.geedgenetworks.core.connector.print;

import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.connector.format.EncodingFormat;
import com.geedgenetworks.core.types.StructType;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
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

    public PrintSinkFunction(StructType dataType, EncodingFormat encodingFormat, PrintMode printMode) {
        this.dataType = dataType;
        this.serialization = encodingFormat.createRuntimeEncoder(dataType);
        this.printMode = printMode;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        serialization.open(null);
    }

    @Override
    public void invoke(Event value, Context context) throws Exception {
        byte[] bytes = serialization.serialize(value);
        String msg = new String(bytes, StandardCharsets.UTF_8);
        if(printMode == STDOUT){
            System.out.println(msg);
        } else if (printMode == LOG_INFO) {
            LOG.info(msg);
        } else if (printMode == LOG_WARN) {
            LOG.warn(msg);
        }
    }
}
