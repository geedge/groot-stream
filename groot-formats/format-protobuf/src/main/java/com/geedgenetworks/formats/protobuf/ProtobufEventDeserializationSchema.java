package com.geedgenetworks.formats.protobuf;

import com.geedgenetworks.shaded.com.google.protobuf.Descriptors.Descriptor;
import com.geedgenetworks.api.connector.serialization.MapDeserialization;
import com.geedgenetworks.api.event.Event;
import com.geedgenetworks.api.connector.type.StructType;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.Base64;
import java.util.Map;

public class ProtobufEventDeserializationSchema implements DeserializationSchema<Event>, MapDeserialization {
    private static final Logger LOG = LoggerFactory.getLogger(ProtobufEventDeserializationSchema.class);
    private final String messageName;
    private final byte[] binaryFileDescriptorSet;
    private final StructType dataType;
    private final boolean ignoreParseErrors;
    private final boolean emitDefaultValues;
    transient private SchemaConverters.MessageConverter converter;

    public ProtobufEventDeserializationSchema(String messageName, byte[] binaryFileDescriptorSet, StructType dataType, boolean ignoreParseErrors, boolean emitDefaultValues) {
        this.messageName = messageName;
        this.binaryFileDescriptorSet = binaryFileDescriptorSet;
        this.dataType = dataType;
        this.ignoreParseErrors = ignoreParseErrors;
        this.emitDefaultValues = emitDefaultValues;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        Descriptor descriptor = ProtobufUtils.buildDescriptor(binaryFileDescriptorSet, messageName);
        this.converter = new SchemaConverters.MessageConverter(descriptor, dataType, emitDefaultValues);
    }

    @Override
    public Event deserialize(byte[] message) throws IOException {
        try {
            Map<String, Object> map = converter.converter(message);
            Event event = new Event();
            event.setExtractedFields(map);
            return event;
        } catch (Exception e) {
            if(ignoreParseErrors){
                LOG.error(String.format("proto解析失败for:%s", Base64.getEncoder().encodeToString(message)), e);
                return null;
            }else{
                throw new IOException(String.format("proto解析失败for:%s", Base64.getEncoder().encodeToString(message)), e);
            }
        }
    }

    @Override
    public Map<String, Object> deserializeToMap(byte[] message) throws IOException {
        try {
            return converter.converter(message);
        } catch (Exception e) {
            if(ignoreParseErrors){
                LOG.error(String.format("proto解析失败for:%s", Base64.getEncoder().encodeToString(message)), e);
                return null;
            }else{
                throw new IOException(String.format("proto解析失败for:%s", Base64.getEncoder().encodeToString(message)), e);
            }
        }
    }

    @Override
    public boolean isEndOfStream(Event nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return null;
    }
}
