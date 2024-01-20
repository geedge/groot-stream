package com.geedgenetworks.formats.protobuf;

import com.geedgenetworks.core.connector.format.DecodingFormat;
import com.geedgenetworks.core.connector.format.EncodingFormat;
import com.geedgenetworks.core.factories.DecodingFormatFactory;
import com.geedgenetworks.core.factories.EncodingFormatFactory;
import com.geedgenetworks.core.factories.TableFactory;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.core.types.StructType;
import com.geedgenetworks.shaded.com.google.protobuf.Descriptors;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;

import java.util.HashSet;
import java.util.Set;

import static com.geedgenetworks.formats.protobuf.ProtobufOptions.*;

public class ProtobufFormatFactory implements DecodingFormatFactory, EncodingFormatFactory {
    public static final String IDENTIFIER = "protobuf";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public DecodingFormat createDecodingFormat(TableFactory.Context context, ReadableConfig formatOptions) {
        final String messageName = formatOptions.get(MESSAGE_NAME);
        final String descFilePath = formatOptions.get(DESC_FILE_PATH);
        final boolean ignoreParseErrors = formatOptions.get(IGNORE_PARSE_ERRORS);
        final boolean emitDefaultValues = formatOptions.get(EMIT_DEFAULT_VALUES);
        final byte[] fileContent = ProtobufUtils.readDescriptorFileContent(descFilePath);
        return new DecodingFormat() {
            @Override
            public DeserializationSchema<Event> createRuntimeDecoder(StructType dataType) {
                try {
                    Descriptors.Descriptor descriptor = ProtobufUtils.buildDescriptor(fileContent, messageName);
                    if(dataType == null) {
                        dataType = SchemaConverters.toStructType(descriptor);
                    }
                    ProtobufUtils.checkSupportParseDescriptor(descriptor);
                    SchemaConverters.checkMatch(descriptor, dataType);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return new ProtobufEventDeserializationSchema(messageName, fileContent, dataType, ignoreParseErrors, emitDefaultValues);
            }
        };
    }

    @Override
    public EncodingFormat createEncodingFormat(TableFactory.Context context, ReadableConfig formatOptions) {
        final String messageName = formatOptions.get(MESSAGE_NAME);
        final String descFilePath = formatOptions.get(DESC_FILE_PATH);
        final byte[] fileContent = ProtobufUtils.readDescriptorFileContent(descFilePath);
        try {
            ProtobufUtils.checkSupportParseDescriptor(ProtobufUtils.buildDescriptor(fileContent, messageName));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return new EncodingFormat() {
            @Override
            public SerializationSchema<Event> createRuntimeEncoder(StructType dataType) {
                return new ProtobufEventSerializationSchema(messageName, fileContent);
            }
        };
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(MESSAGE_NAME);
        options.add(DESC_FILE_PATH);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(IGNORE_PARSE_ERRORS);
        options.add(EMIT_DEFAULT_VALUES);
        return options;
    }
}
