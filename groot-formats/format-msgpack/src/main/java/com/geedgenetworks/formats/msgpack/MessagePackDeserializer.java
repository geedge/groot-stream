package com.geedgenetworks.formats.msgpack;

import com.geedgenetworks.api.connector.type.StructType;
import com.geedgenetworks.api.connector.type.*;
import org.msgpack.core.MessageFormat;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ValueType;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class MessagePackDeserializer implements Serializable{
    private final StructType dataType;
    private final ValueConverter rootConverter; // 带Schema时的converter

    private static final ValueConverter[] converterTable = new ValueConverter[12]; // 无Schema时的converter


    public MessagePackDeserializer(StructType dataType) {
        this.dataType = dataType;
        this.rootConverter = dataType == null ? null : makeConverterForMap(dataType);
    }

    static {
        initConverterTable();
    }

    public Map<String, Object> deserialize(byte[] bytes) throws Exception {
        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(bytes);
        try {
            if(rootConverter == null){
                return MessagePackDeserializer.converterMap(unpacker, null);
            }else{
                return (Map<String, Object>) rootConverter.convert(unpacker, null);
            }
        } finally {
            unpacker.close();
        }
    }

    private ValueConverter[] makeConverter(DataType dataType) {
        ValueConverter[] converterTable = new ValueConverter[12];

        converterTable[ValueType.BOOLEAN.ordinal()] = makeConverterForBoolean(dataType);
        converterTable[ValueType.INTEGER.ordinal()] = makeConverterForInteger(dataType);
        converterTable[ValueType.FLOAT.ordinal()] = makeConverterForFloat(dataType);
        converterTable[ValueType.STRING.ordinal()] = makeConverterForString(dataType);
        converterTable[ValueType.BINARY.ordinal()] = makeConverterForBinary(dataType);
        converterTable[ValueType.ARRAY.ordinal()] = makeConverterForArray(dataType);
        converterTable[ValueType.MAP.ordinal()] = makeConverterForMap(dataType);

        return converterTable;
    }

    public ValueConverter makeConverterForBoolean(DataType dataType){
        if (dataType instanceof BooleanType) {
            return (unpacker, format) -> unpacker.unpackBoolean();
        } else if (dataType instanceof IntegerType) {
            return (unpacker, format) -> unpacker.unpackBoolean() ? 1 : 0;
        } else {
            //throw newCanNotConvertException(ValueType.BOOLEAN.name(), dataType);
            return (unpacker, format) -> {throw newCanNotConvertException(ValueType.BOOLEAN.name(), dataType);};
        }
    }

    public ValueConverter makeConverterForInteger(DataType dataType) {
        if (dataType instanceof IntegerType) {
            return (unpacker, format) -> {
                switch (format) {
                    case UINT64:
                        return unpacker.unpackBigInteger().intValue();
                    case INT64:
                    case UINT32:
                        return (int)unpacker.unpackLong();
                    default:
                        return unpacker.unpackInt();
                }
            };
        } else if (dataType instanceof LongType) {
            return (unpacker, format) -> {
                switch (format) {
                    case UINT64:
                        return unpacker.unpackBigInteger().longValue();
                    case INT64:
                    case UINT32:
                        return unpacker.unpackLong();
                    default:
                        return (long)unpacker.unpackInt();
                }
            };
        } else if (dataType instanceof FloatType) {
            return (unpacker, format) -> {
                switch (format) {
                    case UINT64:
                        return unpacker.unpackBigInteger().floatValue();
                    case INT64:
                    case UINT32:
                        return (float)unpacker.unpackLong();
                    default:
                        return (float)unpacker.unpackInt();
                }
            };
        } else if (dataType instanceof DoubleType) {
            return (unpacker, format) -> {
                switch (format) {
                    case UINT64:
                        return unpacker.unpackBigInteger().doubleValue();
                    case INT64:
                    case UINT32:
                        return (double)unpacker.unpackLong();
                    default:
                        return (double)unpacker.unpackInt();
                }
            };
        } else if (dataType instanceof StringType) {
            return (unpacker, format) -> {
                switch (format) {
                    case UINT64:
                        return unpacker.unpackBigInteger().toString();
                    case INT64:
                    case UINT32:
                        return Long.toString(unpacker.unpackLong());
                    default:
                        return Integer.toString(unpacker.unpackInt());
                }
            };
        } else {
            return (unpacker, format) -> {throw newCanNotConvertException(ValueType.INTEGER.name(), dataType);};
        }
    }

    public ValueConverter makeConverterForFloat(DataType dataType) {
        if (dataType instanceof DoubleType) {
            return (unpacker, format) -> unpacker.unpackDouble();
        } else if (dataType instanceof FloatType) {
            return (unpacker, format) -> (float) unpacker.unpackDouble();
        } else if (dataType instanceof IntegerType) {
            return (unpacker, format) ->  (int) unpacker.unpackDouble();
        } else if (dataType instanceof LongType) {
            return (unpacker, format) ->  (long) unpacker.unpackDouble();
        } else if (dataType instanceof StringType) {
            return (unpacker, format) -> Double.toString(unpacker.unpackDouble());
        } else {
            return (unpacker, format) -> {throw newCanNotConvertException(ValueType.FLOAT.name(), dataType);};
        }
    }

    public ValueConverter makeConverterForString(DataType dataType) {
        if (dataType instanceof StringType) {
            return (unpacker, format) -> unpacker.unpackString();
        } else if (dataType instanceof IntegerType) {
            return (unpacker, format) -> Integer.parseInt(unpacker.unpackString());
        } else if (dataType instanceof LongType) {
            return (unpacker, format) -> Long.parseLong(unpacker.unpackString());
        } else if (dataType instanceof FloatType) {
            return (unpacker, format) -> Float.parseFloat(unpacker.unpackString());
        } else if (dataType instanceof DoubleType) {
            return (unpacker, format) -> Double.parseDouble(unpacker.unpackString());
        } else if (dataType instanceof BinaryType) {
            return (unpacker, format) -> unpacker.readPayload(unpacker.unpackRawStringHeader());
        } else {
            return (unpacker, format) -> {throw newCanNotConvertException(ValueType.STRING.name(), dataType);};
        }
    }

    public ValueConverter makeConverterForBinary(DataType dataType){
        if (dataType instanceof BinaryType) {
            return (unpacker, format) -> unpacker.readPayload(unpacker.unpackBinaryHeader());
        } else {
            return (unpacker, format) -> {throw newCanNotConvertException(ValueType.BINARY.name(), dataType);};
        }
    }

    public ValueConverter makeConverterForArray(DataType dataType) {
        if (dataType instanceof ArrayType) {
            ValueConverter[] converterTable = makeConverter(((ArrayType) dataType).elementType);
            return (unpacker, format) -> {
                int size = unpacker.unpackArrayHeader();
                List<Object> array = new ArrayList<>(size);
                MessageFormat mf;
                ValueType type;
                ValueConverter valueConverter;
                for (int i = 0; i < size; i++) {
                    mf = unpacker.getNextFormat();
                    type = mf.getValueType();
                    if (type == ValueType.NIL) {
                        unpacker.unpackNil();
                        array.add(null);
                        continue;
                    }
                    valueConverter = converterTable[type.ordinal()];
                    if (valueConverter == null) {
                        throw new UnsupportedOperationException(type.name());
                    }
                    array.add(valueConverter.convert(unpacker, mf));
                }
                return array;
            };
        } else {
            return (unpacker, format) -> {throw newCanNotConvertException(ValueType.ARRAY.name(), dataType);};
        }
    }

    public ValueConverter makeConverterForMap(DataType dataType){
        if (!(dataType instanceof StructType)) {
            return (unpacker, format) -> {throw newCanNotConvertException(ValueType.MAP.name(), dataType);};
        }
        final Map<String, ValueConverter[]> filedConverters = Arrays.stream(((StructType) dataType).fields).collect(Collectors.toMap(f -> f.name, f -> this.makeConverter(f.dataType)));
        return (unpacker, format) -> {
            int size = unpacker.unpackMapHeader();
            Map<String, Object> map = new HashMap<>((int) (size / 0.75));
            MessageFormat mf;
            ValueType type;
            ValueConverter[] converterTable;
            ValueConverter valueConverter;

            String key;
            Object value;
            for (int i = 0; i < size; i++) {
                key = unpacker.unpackString();
                converterTable = filedConverters.get(key);
                if(converterTable == null){
                    unpacker.skipValue();
                    continue;
                }

                mf = unpacker.getNextFormat();
                type = mf.getValueType();
                if (type == ValueType.NIL) {
                    unpacker.unpackNil();
                    continue;
                }
                valueConverter = converterTable[type.ordinal()];
                if (valueConverter == null) {
                    throw new UnsupportedOperationException(type.name());
                }
                value = valueConverter.convert(unpacker, mf);
                map.put(key, value);
            }

            return map;
        };
    }

    private static void initConverterTable() {
        converterTable[ValueType.BOOLEAN.ordinal()] = MessagePackDeserializer::converterBoolean;
        converterTable[ValueType.INTEGER.ordinal()] = MessagePackDeserializer::converterInteger;
        converterTable[ValueType.FLOAT.ordinal()] = MessagePackDeserializer::converterFloat;
        converterTable[ValueType.STRING.ordinal()] = MessagePackDeserializer::converterString;
        converterTable[ValueType.BINARY.ordinal()] = MessagePackDeserializer::converterBinary;
        converterTable[ValueType.ARRAY.ordinal()] = MessagePackDeserializer::converterArray;
        converterTable[ValueType.MAP.ordinal()] = MessagePackDeserializer::converterMap;
    }

    public static Object converterBoolean(MessageUnpacker unpacker, MessageFormat format) throws Exception {
        return unpacker.unpackBoolean();
    }

    public static Object converterInteger(MessageUnpacker unpacker, MessageFormat format) throws Exception {
        switch (format) {
            case UINT64:
                return unpacker.unpackBigInteger().longValue();
            case INT64:
            case UINT32:
                return unpacker.unpackLong();
            default:
                return unpacker.unpackInt();
        }
    }

    public static Object converterFloat(MessageUnpacker unpacker, MessageFormat format) throws Exception {
        return unpacker.unpackDouble();
    }

    public static Object converterString(MessageUnpacker unpacker, MessageFormat format) throws Exception {
        return unpacker.unpackString();
    }

    public static Object converterBinary(MessageUnpacker unpacker, MessageFormat format) throws Exception {
        return unpacker.readPayload(unpacker.unpackBinaryHeader());
    }

    public static Object converterArray(MessageUnpacker unpacker, MessageFormat format) throws Exception {
        int size = unpacker.unpackArrayHeader();
        List<Object> array = new ArrayList<>(size);
        MessageFormat mf;
        ValueType type;
        ValueConverter valueConverter;
        for (int i = 0; i < size; i++) {
            mf = unpacker.getNextFormat();
            type = mf.getValueType();
            if (type == ValueType.NIL) {
                unpacker.unpackNil();
                array.add(null);
                continue;
            }
            valueConverter = converterTable[type.ordinal()];
            if (valueConverter == null) {
                throw new UnsupportedOperationException(type.name());
            }
            array.add(valueConverter.convert(unpacker, mf));
        }
        return array;
    }

    public static Map<String, Object> converterMap(MessageUnpacker unpacker, MessageFormat format) throws Exception {
        int size = unpacker.unpackMapHeader();
        Map<String, Object> map = new HashMap<>((int) (size / 0.75));
        MessageFormat mf;
        ValueType type;
        ValueConverter valueConverter;

        String key;
        Object value;
        for (int i = 0; i < size; i++) {
            key = unpacker.unpackString();
            mf = unpacker.getNextFormat();
            type = mf.getValueType();
            if (type == ValueType.NIL) {
                unpacker.unpackNil();
                continue;
            }
            valueConverter = converterTable[type.ordinal()];
            if (valueConverter == null) {
                throw new UnsupportedOperationException(type.name());
            }
            value = valueConverter.convert(unpacker, mf);
            map.put(key, value);
        }

        return map;
    }

    private static IllegalArgumentException newCanNotConvertException(String type, DataType dataType) {
        return new IllegalArgumentException(String.format("%s can not convert to type:%s", type, dataType));
    }

    @FunctionalInterface
    public interface ValueConverter extends Serializable {
        Object convert(MessageUnpacker unpacker, MessageFormat format) throws Exception;
    }
}
