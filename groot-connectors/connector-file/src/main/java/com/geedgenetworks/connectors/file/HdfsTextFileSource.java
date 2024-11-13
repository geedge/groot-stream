package com.geedgenetworks.connectors.file;

import com.geedgenetworks.api.event.Event;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class HdfsTextFileSource extends RichParallelSourceFunction<Event> {
    private static final Logger LOG = LoggerFactory.getLogger(HdfsTextFileSource.class);
    private final DeserializationSchema<Event> deserialization;
    private final String path;
    private final int rowsPerSecond;
    private final long numberOfRows;
    private final long millisPerRow;
    private transient FileSystem fs;
    private volatile boolean stop;

    protected HdfsTextFileSource(DeserializationSchema<Event> deserialization, String path, int rowsPerSecond, long numberOfRows, long millisPerRow) {
        this.deserialization = deserialization;
        this.path = path;
        this.rowsPerSecond = rowsPerSecond;
        this.numberOfRows = numberOfRows;
        this.millisPerRow = millisPerRow;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        fs = new Path(path).getFileSystem(new org.apache.hadoop.conf.Configuration());
        Preconditions.checkArgument(fs.isFile(new Path(path)), "%s is not file", path);
    }

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        final long rowsForSubtask = getRowsForSubTask();
        final long rowsPerSecondForSubtask = getRowsPerSecondForSubTask();

        Event event;
        long rows = 0;
        int batchRows = 0;
        byte[] bytes;
        long batchStartTs = System.currentTimeMillis();
        long batchWait;

        while (!stop && rows < rowsForSubtask) {
            try(InputStream inputStream = fs.open(new Path(path))){
                LineIterator lines = IOUtils.lineIterator(inputStream, "utf-8");
                while (!stop && lines.hasNext() && rows < rowsForSubtask) {
                    String line = lines.next().trim();
                    if(line.isEmpty()){
                        continue;
                    }
                    bytes = line.getBytes(StandardCharsets.UTF_8);
                    try {
                        event = deserialization.deserialize(bytes);
                        event.getExtractedFields().put(Event.INTERNAL_TIMESTAMP_KEY, System.currentTimeMillis());
                        ctx.collect(event);
                        rows += 1;
                    } catch (Exception e) {
                        LOG.error("deserialize error for:" + line, e);
                        continue;
                    }

                    if(millisPerRow > 0){
                        Thread.sleep(millisPerRow);
                    }else{
                        batchRows += 1;
                        if(batchRows >= rowsPerSecondForSubtask){
                            batchRows = 0;
                            batchWait = 1000L - (System.currentTimeMillis() - batchStartTs);
                            if(batchWait > 0) {
                                Thread.sleep(batchWait);
                            }
                            batchStartTs = System.currentTimeMillis();
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
