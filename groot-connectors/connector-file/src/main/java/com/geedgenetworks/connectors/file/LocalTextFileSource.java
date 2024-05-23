package com.geedgenetworks.connectors.file;

import com.geedgenetworks.common.Event;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class LocalTextFileSource extends RichParallelSourceFunction<Event> {
    private final DeserializationSchema<Event> deserialization;
    private final String path;
    private final int rowsPerSecond;
    private final long numberOfRows;
    private final long millisPerRow;
    private volatile boolean stop;

    protected LocalTextFileSource(DeserializationSchema<Event> deserialization, String path, int rowsPerSecond, long numberOfRows, long millisPerRow) {
        this.deserialization = deserialization;
        this.path = path;
        this.rowsPerSecond = rowsPerSecond;
        this.numberOfRows = numberOfRows;
        this.millisPerRow = millisPerRow;
    }

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        final long rowsForSubtask = getRowsForSubTask();
        final long rowsPerSecondForSubtask = getRowsPerSecondForSubTask();

        long rows = 0;
        int batchRows = 0;
        byte[] bytes;
        long nextReadTime = System.currentTimeMillis();
        long waitMs;

        while (!stop && rows < rowsForSubtask) {
            try(InputStream inputStream = new FileInputStream(path)){
                LineIterator lines = IOUtils.lineIterator(inputStream, "utf-8");
                while (!stop && lines.hasNext() && rows < rowsForSubtask) {
                    String line = lines.next().trim();
                    if(line.isEmpty()){
                        continue;
                    }
                    bytes = line.getBytes(StandardCharsets.UTF_8);
                    ctx.collect(deserialization.deserialize(bytes));
                    rows += 1;

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
            }
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
