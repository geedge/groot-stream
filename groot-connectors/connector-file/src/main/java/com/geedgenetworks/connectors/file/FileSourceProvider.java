package com.geedgenetworks.connectors.file;

import com.geedgenetworks.spi.source.SourceProvider;
import com.geedgenetworks.spi.table.event.Event;
import com.geedgenetworks.spi.table.type.StructType;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class FileSourceProvider implements SourceProvider {
    private final StructType physicalDataType;
    private final DeserializationSchema<Event> deserialization;

    private final String path;
    private final boolean readLocalFileInClient;
    private final int rowsPerSecond;
    private final long numberOfRows;
    private final long millisPerRow;

    public FileSourceProvider(StructType physicalDataType, DeserializationSchema<Event> deserialization, String path, boolean readLocalFileInClient, int rowsPerSecond, long numberOfRows, long millisPerRow) {
        this.physicalDataType = physicalDataType;
        this.deserialization = deserialization;
        this.path = path;
        this.readLocalFileInClient = readLocalFileInClient;
        this.rowsPerSecond = rowsPerSecond;
        this.numberOfRows = numberOfRows;
        this.millisPerRow = millisPerRow;
    }

    @Override
    public SingleOutputStreamOperator<Event> produceDataStream(StreamExecutionEnvironment env) {
        boolean isLocalPath = !path.startsWith("hdfs://");

        SourceFunction<Event> sourceFunction = null;
        if (isLocalPath) {
            if (readLocalFileInClient) {
                byte[] lineBytes = getLocalTextFileLineBytes(path);
                sourceFunction = new MemoryTextFileSource(deserialization, lineBytes, rowsPerSecond, numberOfRows, millisPerRow);
            } else {
                sourceFunction = new LocalTextFileSource(deserialization, path, rowsPerSecond, numberOfRows, millisPerRow);
            }
        } else {
            sourceFunction = new HdfsTextFileSource(deserialization, path, rowsPerSecond, numberOfRows, millisPerRow);
        }

        return env.addSource(sourceFunction);
    }

    @Override
    public StructType getPhysicalDataType() {
        return physicalDataType;
    }

    private byte[] getLocalTextFileLineBytes(String path) {
        try {
            File file = new File(path);
            long fileLength = file.length();
            if(fileLength > (1 << 20) * 128){
                throw new IllegalArgumentException(String.format("file:%s size is bigger than 128MB"));
            }

            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            byte[] intBytes = new byte[4];
            byte[] bytes;
            try(InputStream inputStream = new FileInputStream(file)){
                LineIterator lines = IOUtils.lineIterator(inputStream, "utf-8");
                while (lines.hasNext()) {
                    String line = lines.next().trim();
                    if(line.isEmpty()){
                        continue;
                    }
                    bytes = line.getBytes(StandardCharsets.UTF_8);
                    intBytes[0] = (byte) (bytes.length >> 24);
                    intBytes[1] = (byte) (bytes.length >> 16);
                    intBytes[2] = (byte) (bytes.length >> 8);
                    intBytes[3] = (byte) bytes.length;
                    outputStream.write(intBytes);
                    outputStream.write(bytes);
                }
            }

            return outputStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
