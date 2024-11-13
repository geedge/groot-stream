package com.geedgenetworks.connectors.file;

import com.geedgenetworks.api.connector.event.Event;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class MemoryTextFileSource extends RichParallelSourceFunction<Event> {
    private static final Logger LOG = LoggerFactory.getLogger(MemoryTextFileSource.class);
    private final DeserializationSchema<Event> deserialization;
    private final byte[] lineBytes;
    private final int rowsPerSecond;
    private final long numberOfRows;
    private final long millisPerRow;
    private volatile boolean stop;

    protected MemoryTextFileSource(DeserializationSchema<Event> deserialization, byte[] lineBytes, int rowsPerSecond, long numberOfRows, long millisPerRow) {
        this.deserialization = deserialization;
        this.lineBytes = lineBytes;
        this.rowsPerSecond = rowsPerSecond;
        this.numberOfRows = numberOfRows;
        this.millisPerRow = millisPerRow;
    }

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        final long rowsForSubtask = getRowsForSubTask();
        final long rowsPerSecondForSubtask = getRowsPerSecondForSubTask();

        Event event;
        long rows = 0;
        int batchRows = 0;
        byte[] bytes;
        long nextReadTime = System.currentTimeMillis();
        long waitMs;
        ByteBuffer buffer = ByteBuffer.wrap(lineBytes);
        int lineSize;

        while (!stop && rows < rowsForSubtask) {
            while (!stop && buffer.hasRemaining() && rows < rowsForSubtask){
                lineSize = buffer.getInt();
                bytes = new byte[lineSize];
                buffer.get(bytes);
                try {
                    event = deserialization.deserialize(bytes);
                    event.getExtractedFields().put(Event.INTERNAL_TIMESTAMP_KEY, System.currentTimeMillis());
                    ctx.collect(event);
                    rows += 1;
                } catch (Exception e) {
                    LOG.error("deserialize error for:" + new String(bytes, StandardCharsets.UTF_8), e);
                    continue;
                }

                if(millisPerRow > 0){
                    Thread.sleep(millisPerRow);
                }else{
                    batchRows += 1;
                    if(batchRows >= rowsPerSecondForSubtask){
                        batchRows = 0;
                        nextReadTime += 1000;
                        waitMs = Math.max(0, nextReadTime - System.currentTimeMillis());
                        if(waitMs > 0) {
                            Thread.sleep(waitMs);
                        }
                    }
                }
            }
            buffer.clear();
        }

    }

    private long getRowsPerSecondForSubTask() {
        int numSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        long baseRowsPerSecondPerSubtask = rowsPerSecond / numSubtasks;
        return (rowsPerSecond % numSubtasks > indexOfThisSubtask) ? baseRowsPerSecondPerSubtask + 1 : baseRowsPerSecondPerSubtask;
    }

    private long getRowsForSubTask() {
        if (numberOfRows < 0) {
            return Long.MAX_VALUE;
        } else {
            int numSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            final long baseNumOfRowsPerSubtask = numberOfRows / numSubtasks;
            return (numberOfRows % numSubtasks > indexOfThisSubtask) ? baseNumOfRowsPerSubtask + 1 : baseNumOfRowsPerSubtask;
        }
    }

    @Override
    public void cancel() {
        stop = true;
    }
}
