package com.geedgenetworks.connectors.ipfix.collector;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.geedgenetworks.core.connector.source.SourceProvider;
import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.types.*;
import com.geedgenetworks.core.types.DataType;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.cert.netsa.io.ipfix.*;
import scala.collection.Iterator;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

import static com.geedgenetworks.connectors.ipfix.collector.IPFixConnectorOptionsUtil.getServiceDiscoveryProperties;

public class IPFixSourceProvider implements SourceProvider {
    private static final Log logger = LogFactory.get();

    private final StructType physicalDataType;
    private final ReadableConfig config;
    private final Map<String, String> options;
    private final Map<String, ValueConverter> filedConverters;
    private static SessionGroup sessionGroup;

    public IPFixSourceProvider(StructType physicalDataType, ReadableConfig config, Map<String, String> options, SessionGroup sessionGroup) {
        this.physicalDataType = physicalDataType;
        this.config = config;
        this.options = options;
        IPFixSourceProvider.sessionGroup = sessionGroup;
        this.filedConverters = physicalDataType != null ? Arrays.stream(physicalDataType.fields).collect(Collectors.toMap(f -> f.name, f -> this.makeConverter(f.dataType))) : null;
    }

    @Override
    public SingleOutputStreamOperator<Event> produceDataStream(StreamExecutionEnvironment env) {
        Properties serviceDiscoveryProperties = getServiceDiscoveryProperties(options);
        UDPSource udpSource = new UDPSource(config.get(IPFixConnectorOptions.INITIAL_PORT),
                config.get(IPFixConnectorOptions.MAX_ATTEMPTS),
                config.get(IPFixConnectorOptions.BUFFER_SIZE),
                config.get(IPFixConnectorOptions.RECEIVE_BUFFER),
                serviceDiscoveryProperties);
        DataStream<byte[]> source = env.addSource(udpSource).setParallelism(config.get(IPFixConnectorOptions.PORT_NUMBER));
        return source.flatMap(new FlatMapFunction<byte[], Event>() {
            @Override
            public void flatMap(byte[] data, Collector<Event> out) {
                try {
                    ByteBuffer buffer = ByteBuffer.wrap(data);
                    RecordReader reader = new RecordReader(new ByteBufferMessageReader(buffer, sessionGroup));
                    while (reader.hasNext()) {
                        Event event = new Event();
                        Record record = reader.next();
                        Iterator<Field> fields = record.fields();
                        Map<String, Object> fieldMap = new HashMap<>();
                        while (fields.hasNext()) {
                            Field field = fields.next();
                            String fieldName = field.ie().name();
                            if (filedConverters != null) {
                                ValueConverter valueConverter = filedConverters.get(field.ie().name());
                                if (valueConverter != null) {
                                    fieldMap.put(fieldName, valueConverter.convert(field.value()));
                                }
                            } else {
                                fieldMap.put(fieldName, field.value());
                            }
                        }
                        fieldMap.put(Event.INTERNAL_TIMESTAMP_KEY, record.exportTime().get().getEpochSecond() * 1000);
                        event.setExtractedFields(fieldMap);
                        event.setFinal(true);
                        out.collect(event);
                    }
                } catch (NoTemplateException e) {
                    logger.error(e.getMessage()+". The client did not send the template, or there was a delay in processing the template message." );
                } catch (Exception e) {
                    logger.error("IPFix parsing failed.", e);
                }
            }
        });
    }

    @Override
    public StructType getPhysicalDataType() {
        return physicalDataType;
    }

    private ValueConverter makeConverter(DataType dataType) {
        if (dataType instanceof StringType) {
            return this::convertToString;
        }
        if (dataType instanceof IntegerType) {
            return this::convertToInteger;
        }
        if (dataType instanceof LongType) {
            return this::convertToLong;
        }
        throw new UnsupportedOperationException("unsupported dataType: " + dataType);
    }

    private Object convertToString(Object data) throws Exception {
        return String.valueOf(data);
    }

    private Object convertToInteger(Object data) throws Exception {
        if (data instanceof Integer) {
            return data;
        }
        if (data instanceof Long) {
            return Integer.parseInt(data.toString());
        }
        if (data instanceof String) {
            return Integer.parseInt(data.toString());
        }
        return null;
    }

    private Object convertToLong(Object data) throws Exception {
        if (data instanceof Long) {
            return data;
        }
        if (data instanceof Integer) {
            return Long.parseLong(data.toString());
        }
        if (data instanceof String) {
            return Long.parseLong(data.toString());
        }
        return null;
    }

    @FunctionalInterface
    public interface ValueConverter extends Serializable {
        Object convert(Object data) throws Exception;
    }
}
