package com.geedgenetworks.connectors.mock;

import com.geedgenetworks.connectors.mock.faker.ObjectFaker;
import com.geedgenetworks.api.connector.event.Event;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Map;

public class MockSource extends RichParallelSourceFunction<Event> {
    private final ObjectFaker faker;
    private final int rowsPerSecond;
    private final long numberOfRows;
    private final long millisPerRow;
    private volatile boolean stop;

    public MockSource(ObjectFaker faker, int rowsPerSecond, long numberOfRows, long millisPerRow) {
        this.faker = faker;
        this.rowsPerSecond = rowsPerSecond;
        this.numberOfRows = numberOfRows;
        this.millisPerRow = millisPerRow;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        faker.init(getRuntimeContext().getNumberOfParallelSubtasks(), getRuntimeContext().getIndexOfThisSubtask());
    }

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        final long rowsForSubtask = getRowsForSubTask();
        final int rowsPerSecondForSubtask = getRowsPerSecondForSubTask();

        Event event;
        Map<String, Object> value;
        long rows = 0;
        int batchRows = 0;
        long nextReadTime = System.currentTimeMillis();
        long waitMs;

        while (!stop && rows < rowsForSubtask) {
            while (!stop  && rows < rowsForSubtask){
                event = new Event();
                value = faker.geneValue();
                value.put(Event.INTERNAL_TIMESTAMP_KEY, System.currentTimeMillis());
                event.setExtractedFields(value);
                ctx.collect(event);
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

    @Override
    public void close() throws Exception {
        faker.destroy();
    }

    @Override
    public void cancel() {
        stop = true;
    }

    private int getRowsPerSecondForSubTask() {
        int numSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
        int baseRowsPerSecondPerSubtask = rowsPerSecond / numSubtasks;
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
}
