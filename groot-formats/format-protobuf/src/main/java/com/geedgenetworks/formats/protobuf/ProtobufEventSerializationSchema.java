package com.geedgenetworks.formats.protobuf;

import com.alibaba.fastjson2.JSON;
import com.geedgenetworks.core.pojo.Event;
import com.google.protobuf.Descriptors;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ProtobufEventSerializationSchema implements SerializationSchema<Event> {
    private static final Logger LOG = LoggerFactory.getLogger(ProtobufEventSerializationSchema.class);
    private final String messageName;
    private final byte[] binaryFileDescriptorSet;
    transient private ProtobufSerializer serializer;

    public ProtobufEventSerializationSchema(String messageName, byte[] binaryFileDescriptorSet) {
        this.messageName = messageName;
        this.binaryFileDescriptorSet = binaryFileDescriptorSet;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        Descriptors.Descriptor descriptor = ProtobufUtils.buildDescriptor(binaryFileDescriptorSet, messageName);
        serializer = new ProtobufSerializer(descriptor);
    }

    @Override
    public byte[] serialize(Event element) {
        Map<String, Object> map = element.getExtractedFields();
        try {
            return serializer.serialize(map);
        } catch (Exception e) {
            LOG.error(String.format("proto serialize失败for:%s", JSON.toJSONString(map)), e);
            return null;
        }
    }
}
