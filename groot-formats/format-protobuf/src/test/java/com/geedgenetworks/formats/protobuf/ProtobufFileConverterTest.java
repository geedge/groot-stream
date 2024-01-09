package com.geedgenetworks.formats.protobuf;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.geedgenetworks.shaded.com.google.protobuf.Descriptors;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.flink.shaded.guava18.com.google.common.io.Files;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class ProtobufFileConverterTest {

    public static void main(String[] args) throws Exception{
        //json2Proto();
        proto2Json();
    }

    public static void json2Proto() throws Exception{
        String path = ProtobufFileConverterTest.class.getResource("/session_record_test.desc").getPath();
        Descriptors.Descriptor descriptor = ProtobufUtils.buildDescriptor(ProtobufUtils.readDescriptorFileContent(path),"SessionRecord");
        ProtobufSerializer serializer = new ProtobufSerializer(descriptor);

        String filePath = "D:\\doc\\groot\\SESSION-RECORD-24-0108.proto";
        RandomAccessFile raf = new RandomAccessFile(filePath, "rw");
        try(FileInputStream inputStream = new FileInputStream("D:\\doc\\groot\\SESSION-RECORD-24-0108.json")){
            LineIterator lines = IOUtils.lineIterator(inputStream, "utf-8");
            while (lines.hasNext()){
                String line = lines.next().trim();
                if(line.isEmpty()){
                    continue;
                }
                JSONObject map = JSON.parseObject(line);
                byte[] bytes = serializer.serialize(map);
                raf.writeInt(bytes.length);
                raf.write(bytes);
            }
        }
        raf.close();
    }

    public static void proto2Json() throws Exception{
        String path = ProtobufFileConverterTest.class.getResource("/session_record_test.desc").getPath();
        Descriptors.Descriptor descriptor = ProtobufUtils.buildDescriptor(ProtobufUtils.readDescriptorFileContent(path),"SessionRecord");
        SchemaConverters.MessageConverter messageConverter = new SchemaConverters.MessageConverter(descriptor, SchemaConverters.toStructType(descriptor), false);

        RandomAccessFile raf = new RandomAccessFile("D:\\doc\\groot\\SESSION-RECORD-24-0108-covert.json", "rw");
        String filePath = "D:\\doc\\groot\\SESSION-RECORD-24-0108.proto";
        MappedByteBuffer byteBuffer = Files.map(new File(filePath));

        int limit = byteBuffer.limit();
        int size;
        int read = 0;
        while (read < limit){
            size = byteBuffer.getInt();
            read += size + 4;
            byte[] bytes = new byte[size];
            byteBuffer.get(bytes, 0, size);
            Map<String, Object> map = messageConverter.converter(bytes);
            raf.write(JSON.toJSONString(map).getBytes(StandardCharsets.UTF_8));
            raf.write('\n');
        }

        raf.close();
    }
}
