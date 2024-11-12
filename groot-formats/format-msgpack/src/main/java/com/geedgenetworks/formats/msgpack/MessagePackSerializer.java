package com.geedgenetworks.formats.msgpack;

import com.alibaba.fastjson2.JSON;
import com.geedgenetworks.api.connector.type.*;
import org.apache.commons.io.IOUtils;
import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MessagePackSerializer implements Serializable {
    private final StructType dataType;
    private final ValueWriter valueWriter;
    private ArrayDeque<MessageBufferPacker> bufferPackers;

    public MessagePackSerializer(StructType dataType) {
        this.dataType = dataType;
        this.valueWriter = dataType == null ? null : makeWriter(dataType);
        this.bufferPackers = new ArrayDeque<>();
    }

    public byte[] serialize(Map<String, Object> data){
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        try {
            if (dataType == null) {
                writeMapValue(packer, data);
                return packer.toByteArray();
            } else {
                valueWriter.write(packer, data);
                return packer.toByteArray();
            }
        } catch (Exception e){
            throw new RuntimeException(e);
        } finally {
            //packer.close();
            IOUtils.closeQuietly(packer);
        }
    }

    private ValueWriter makeWriter(DataType dataType) {
        if (dataType instanceof StringType) {
            return this::writeString;
        }

        if (dataType instanceof IntegerType) {
            return this::writeInt;
        }

        if (dataType instanceof LongType) {
            return this::writeLong;
        }

        if (dataType instanceof FloatType) {
            return this::writeFloat;
        }

        if (dataType instanceof DoubleType) {
            return this::writeDouble;
        }

        if (dataType instanceof BooleanType) {
            return this::writeBoolean;
        }

        if (dataType instanceof BinaryType) {
            return this::writeBinary;
        }

        if (dataType instanceof StructType) {
            final Map<String, ValueWriter> fieldWriters = Arrays.stream(((StructType) dataType).fields).collect(Collectors.toMap(f -> f.name, f -> this.makeWriter(f.dataType)));
            return (packer, obj) -> {
                if (obj instanceof Map) {
                    writeObject(packer, (Map<String, Object>) obj, fieldWriters);
                } else {
                    throw new IllegalArgumentException(String.format("can not convert %s to map", obj));
                }
            };
        }

        if (dataType instanceof ArrayType) {
            final ValueWriter elementWriter = this.makeWriter(((ArrayType) dataType).elementType);
            return (packer, obj) -> {
                if (obj instanceof List) {
                    writeArray(packer, (List<Object>) obj, elementWriter);
                }
            };
        }

        throw new UnsupportedOperationException("unsupported dataType: " + dataType);
    }

    void writeString(MessagePacker packer, Object obj) throws Exception {
        if (obj instanceof String) {
            packer.packString((String) obj);
        } else if (obj instanceof byte[]) {
            byte[] bytes = (byte[]) obj;
            packer.packRawStringHeader(bytes.length);
            packer.writePayload(bytes);
        } else {
            packer.packString(JSON.toJSONString(obj));
        }
    }

    void writeInt(MessagePacker packer, Object obj) throws Exception {
        if (obj instanceof Number) {
            packer.packInt(((Number) obj).intValue());
        } else if (obj instanceof String) {
            packer.packInt(Integer.parseInt((String) obj));
        } else {
            throw new IllegalArgumentException(String.format("can not convert %s to int", obj));
        }
    }

    void writeLong(MessagePacker packer, Object obj) throws Exception {
        if (obj instanceof Number) {
            packer.packLong(((Number) obj).longValue());
        } else if (obj instanceof String) {
            packer.packLong(Long.parseLong((String) obj));
        } else {
            throw new IllegalArgumentException(String.format("can not convert %s to long", obj));
        }
    }

    void writeFloat(MessagePacker packer, Object obj) throws Exception {
        if (obj instanceof Number) {
            packer.packFloat(((Number) obj).floatValue());
        } else if (obj instanceof String) {
            packer.packFloat(Float.parseFloat((String) obj));
        } else {
            throw new IllegalArgumentException(String.format("can not convert %s to float", obj));
        }
    }

    void writeDouble(MessagePacker packer, Object obj) throws Exception {
        if (obj instanceof Number) {
            packer.packDouble(((Number) obj).doubleValue());
        } else if (obj instanceof String) {
            packer.packDouble(Double.parseDouble((String) obj));
        } else {
            throw new IllegalArgumentException(String.format("can not convert %s to double", obj));
        }
    }

    void writeBoolean(MessagePacker packer, Object obj) throws Exception {
        if (obj instanceof Boolean) {
            packer.packBoolean((Boolean) obj);
        } else if (obj instanceof Number) {
            packer.packBoolean(((Number) obj).intValue() != 0);
        } else {
            throw new IllegalArgumentException(String.format("can not convert %s to bool", obj));
        }
    }

    void writeBinary(MessagePacker packer, Object obj) throws Exception {
        if (obj instanceof byte[]) {
            byte[] bytes = (byte[]) obj;
            packer.packBinaryHeader(bytes.length);
            packer.writePayload(bytes);
        } else if (obj instanceof String) {
            byte[] bytes = obj.toString().getBytes(StandardCharsets.UTF_8);
            packer.packBinaryHeader(bytes.length);
            packer.writePayload(bytes);
        } else {
            throw new IllegalArgumentException(String.format("can not convert %s to byte[]", obj));
        }
    }

    void writeObject(MessagePacker packer, Map<String, Object> map, Map<String, ValueWriter> fieldWriters) throws Exception {
        MessageBufferPacker bufferPacker = getBufferPacker();
        try {
            String key;
            Object value;
            ValueWriter valueWriter;
            int size = 0;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                key = entry.getKey();
                if (key.startsWith("__")) {
                    continue;
                }
                value = entry.getValue();
                if (value == null) {
                    continue;
                }
                valueWriter = fieldWriters.get(key);
                if (valueWriter != null) {
                    bufferPacker.packString(key);
                    valueWriter.write(bufferPacker, value);
                    size++;
                }
            }
            byte[] bytes = bufferPacker.toByteArray();
            packer.packMapHeader(size);
            packer.writePayload(bytes);
        } finally {
            recycleBufferPacker(bufferPacker);
        }
    }

    void writeArray(MessagePacker packer, List<Object> array, ValueWriter elementWriter) throws Exception {
        packer.packArrayHeader(array.size());
        Object value;
        for (int i = 0; i < array.size(); i++) {
            value = array.get(i);
            if (value == null) {
                packer.packNil();
                continue;
            }
            elementWriter.write(packer, value);
        }
    }

    private MessageBufferPacker getBufferPacker() {
        if (bufferPackers.isEmpty()) {
            return MessagePack.newDefaultBufferPacker();
        }

        return bufferPackers.pollLast();
    }

    private void recycleBufferPacker(MessageBufferPacker bufferPacker) {
        bufferPacker.clear();
        bufferPackers.addLast(bufferPacker);
    }

    public void writeValue(MessagePacker packer, Object value) throws Exception {
        if (value instanceof String) {
            packer.packString((String) value);
            return;
        }

        if (value instanceof Integer) {
            packer.packInt((Integer) value);
            return;
        }

        if (value instanceof Long) {
            packer.packLong((Long) value);
            return;
        }

        if (value instanceof Float) {
            packer.packFloat((Float) value);
            return;
        }

        if (value instanceof Double) {
            packer.packDouble((Double) value);
            return;
        }

        if (value instanceof Number) {
            packer.packLong(((Number) value).longValue());
            return;
        }

        if (value instanceof Boolean) {
            packer.packBoolean((Boolean) value);
            return;
        }

        if (value instanceof byte[]) {
            byte[] bytes = (byte[]) value;
            packer.packBinaryHeader(bytes.length);
            packer.writePayload(bytes);
            return;
        }

        if (value instanceof Map) {
            writeMapValue(packer, (Map<String, Object>) value);
            return;
        }

        if (value instanceof List) {
            writeArrayValue(packer, (List<Object>) value);
            return;
        }

        throw new UnsupportedOperationException("can not write class:" + value.getClass());
    }

    public void writeMapValue(MessagePacker packer, Map<String, Object> map) throws Exception {
        MessageBufferPacker bufferPacker = getBufferPacker();
        try {
            String key;
            Object value;
            int size = 0;
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                key = entry.getKey();
                if (key.startsWith("__")) {
                    continue;
                }
                value = entry.getValue();
                if (value == null) {
                    continue;
                }
                bufferPacker.packString(key);
                writeValue(bufferPacker, value);
                size++;
            }
            byte[] bytes = bufferPacker.toByteArray();
            packer.packMapHeader(size);
            packer.writePayload(bytes);
        } finally {
            recycleBufferPacker(bufferPacker);
        }
    }

    public void writeArrayValue(MessagePacker packer, List<Object> array) throws Exception {
        packer.packArrayHeader(array.size());
        Object value;
        for (int i = 0; i < array.size(); i++) {
            value = array.get(i);
            if (value == null) {
                packer.packNil();
                continue;
            }
            writeValue(packer, value);
        }
    }

    @FunctionalInterface
    public interface ValueWriter extends Serializable {
        void write(MessagePacker packer, Object obj) throws Exception;
    }
}
