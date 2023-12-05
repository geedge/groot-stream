package com.geedgenetworks.connectors.ipfix.collector;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.alibaba.fastjson2.JSON;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.config.EngineConfig;
import com.geedgenetworks.connectors.ipfix.collector.source.UDPSource;
import com.geedgenetworks.connectors.ipfix.collector.util.IPFIXUtils;
import com.geedgenetworks.core.connector.source.SourceProvider;
import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.types.*;
import com.geedgenetworks.core.types.DataType;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.cert.netsa.io.ipfix.*;
import scala.collection.Iterator;
import scala.collection.concurrent.TrieMap;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

public class IPFixSourceProvider implements SourceProvider {
    private static final Log logger = LogFactory.get();

    private final StructType physicalDataType;
    private final ReadableConfig config;
    private final Map<String, ValueConverter> filedConverters;
    private static SessionGroup sessionGroup;

    public IPFixSourceProvider(StructType physicalDataType, ReadableConfig config, SessionGroup sessionGroup) {
        this.physicalDataType = physicalDataType;
        this.config = config;
        IPFixSourceProvider.sessionGroup = sessionGroup;
        this.filedConverters = physicalDataType != null ? Arrays.stream(physicalDataType.fields).collect(Collectors.toMap(f -> f.name, f -> this.makeConverter(f.dataType))) : null;
    }

    @Override
    public SingleOutputStreamOperator<Event> produceDataStream(StreamExecutionEnvironment env) {
        ExecutionConfig.GlobalJobParameters executionConfig = env.getConfig().getGlobalJobParameters();
        EngineConfig engineConfig = JSON.parseObject(executionConfig.toMap().get(Constants.SYSPROP_GROOTSTREAM_CONFIG), EngineConfig.class);
        UDPSource udpSource = new UDPSource(config.get(IPFixConnectorOptions.INITIAL_PORT),
                config.get(IPFixConnectorOptions.MAX_ATTEMPTS),
                config.get(IPFixConnectorOptions.BUFFER_SIZE),
                config.get(IPFixConnectorOptions.RECEIVE_BUFFER),
                config.get(IPFixConnectorOptions.SERVICE_DISCOVERY_SERVICE_NAME),
                config.get(IPFixConnectorOptions.SERVICE_DISCOVERY_REGISTER_MODE),
                config.get(IPFixConnectorOptions.SERVICE_DISCOVERY_HEALTH_CHECK_INTERVAL),
                engineConfig.getNacosConfig(),
                engineConfig.getConsulConfig());
        DataStream<byte[]> source = env.addSource(udpSource).setParallelism(config.getOptional(IPFixConnectorOptions.PORT_NUMBER).get());
        return source.flatMap(new FlatMapFunction<byte[], Event>() {
            @Override
            public void flatMap(byte[] data, Collector<Event> out) {
                try {
                    ByteBuffer buffer = ByteBuffer.wrap(data);
                    RecordReader reader = new RecordReader(new ByteBufferMessageReader(buffer, sessionGroup));
                    if (reader.isEmpty()) {
                        Iterator<Session> iterator = sessionGroup.iterator();
                        while (iterator.hasNext()) {
                            Session next = iterator.next();
                            logger.info("domain: " + next.observationDomain());
                            TrieMap<Object, Template> objectTemplateTrieMap = next.idGetTmpl();
                            logger.info("Template: " + objectTemplateTrieMap.toString());
                        }
                    } else {
                        while (reader.hasNext()) {
                            Event event = new Event();
                            Record record = reader.next();
                            Iterator<Field> fields = record.fields();
                            Map<String, Object> fieldMap = new HashMap<>();
                            while (fields.hasNext()) {
                                Field field = fields.next();
                                fieldMap.put(field.ie().name(), field.value());
                            }
                            fieldMap.put(Event.INTERNAL_TIMESTAMP_KEY, record.exportTime().get().getEpochSecond());
                            if (filedConverters != null) {
                                fieldMap = IPFIXUtils.dataConverter(fieldMap, filedConverters);
                            }
                            event.setExtractedFields(fieldMap);
                            event.setFinal(true);
                            out.collect(event);
                        }
                    }
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
