package com.geedgenetworks.formats.csv;

import com.geedgenetworks.api.connector.type.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectReader;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

public class CsvToMapDataConverter implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(CsvToMapDataConverter.class);
    private final StructType dataType;
    private final CsvSchema csvSchema;
    private final boolean ignoreParseErrors;
    private final ValueConverter valueConverter;
    private transient ObjectReader objectReader;

    public CsvToMapDataConverter(StructType dataType, CsvSchema csvSchema, boolean ignoreParseErrors) {
        this.dataType = dataType;
        this.csvSchema = csvSchema;
        this.ignoreParseErrors = ignoreParseErrors;
        this.valueConverter = createRowConverter(dataType, true);
        this.objectReader = new CsvMapper().readerFor(JsonNode.class).with(csvSchema);
    }

    public Map<String, Object> convert(String message) {
        if (objectReader == null) {
            this.objectReader = new CsvMapper().readerFor(JsonNode.class).with(csvSchema);
        }
        try {
            final JsonNode root = objectReader.readValue(message);
            return (Map<String, Object>) valueConverter.convert(root);
        } catch (Throwable t) {
            if (ignoreParseErrors) {
                LOG.error(String.format("CSV Parse Errors:%s", message), t);
                return null;
            }
            throw new UnsupportedOperationException(String.format("CSV Parse Errors:%s", message), t);
        }
    }

    private ValueConverter createRowConverter(StructType rowType, boolean isTopLevel) {
        final ValueConverter[] fieldConverters = Arrays.stream(rowType.fields).map(f -> makeConverter(f.dataType)).toArray(ValueConverter[]::new);
        final String[] fields = Arrays.stream(rowType.fields).map(f -> f.name).toArray(String[]::new);
        final int arity = fields.length;
        return node -> {
            int nodeSize = node.size();

            if (nodeSize != 0) {
                validateArity(arity, nodeSize, ignoreParseErrors);
            } else {
                return null;
            }

            Map<String, Object> obj = new HashMap<>();
            Object value;
            for (int i = 0; i < arity; i++) {
                JsonNode field;
                // Jackson only supports mapping by name in the first level
                if (isTopLevel) {
                    field = node.get(fields[i]);
                } else {
                    field = node.get(i);
                }
                if (field != null && !field.isNull()) {
                    value = fieldConverters[i].convert(field);
                    if (value != null) {
                        obj.put(fields[i], value);
                    }
                }
            }
            return obj;
        };
    }

    private ValueConverter createArrayConverter(ArrayType arrayType) {
        final ValueConverter converter = makeConverter(arrayType.elementType);
        return node -> {
            final ArrayNode arrayNode = (ArrayNode) node;
            if (arrayNode.size() == 0) {
                return null;
            }
            List<Object> objs = new ArrayList<>(arrayNode.size());
            for (int i = 0; i < arrayNode.size(); i++) {
                final JsonNode innerNode = arrayNode.get(i);
                if (innerNode == null || innerNode.isNull()) {
                    objs.add(null);
                }else{
                    objs.add(converter.convert(innerNode));
                }
            }
            return objs;
        };
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

        if (dataType instanceof FloatType) {
            return this::convertToFloat;
        }

        if (dataType instanceof DoubleType) {
            return this::convertToDouble;
        }

        if (dataType instanceof BooleanType) {
            return this::convertToBoolean;
        }

        if (dataType instanceof StructType) {
            return createRowConverter((StructType) dataType, false);
        }

        if (dataType instanceof ArrayType) {
            return createArrayConverter((ArrayType) dataType);
        }

        throw new UnsupportedOperationException("unsupported dataType: " + dataType);
    }

    private String convertToString(JsonNode node) {
        return node.asText();
    }

    private Integer convertToInteger(JsonNode node) {
        if (node.canConvertToInt()) {
            // avoid redundant toString and parseInt, for better performance
            return node.asInt();
        } else {
            String text = node.asText().trim();
            if (StringUtils.isBlank(text)) {
                return null;
            }
            return Integer.parseInt(text);
        }
    }

    private Long convertToLong(JsonNode node) {
        if (node.canConvertToLong()) {
            // avoid redundant toString and parseLong, for better performance
            return node.asLong();
        } else {
            String text = node.asText().trim();
            if (StringUtils.isBlank(text)) {
                return null;
            }
            return Long.parseLong(text);
        }
    }

    private Float convertToFloat(JsonNode node) {
        if (node.isDouble()) {
            // avoid redundant toString and parseDouble, for better performance
            return (float) node.asDouble();
        } else {
            String text = node.asText().trim();
            if (StringUtils.isBlank(text)) {
                return null;
            }
            return Float.parseFloat(text);
        }
    }

    private Double convertToDouble(JsonNode node) {
        if (node.isDouble()) {
            // avoid redundant toString and parseDouble, for better performance
            return node.asDouble();
        } else {
            String text = node.asText().trim();
            if (StringUtils.isBlank(text)) {
                return null;
            }
            return Double.parseDouble(text);
        }
    }

    private Boolean convertToBoolean(JsonNode node) {
        if (node.isBoolean()) {
            // avoid redundant toString and parseBoolean, for better performance
            return node.asBoolean();
        } else {
            String text = node.asText().trim();
            if (StringUtils.isBlank(text)) {
                return null;
            }
            return Boolean.parseBoolean(text);
        }
    }

    private static void validateArity(int expected, int actual, boolean ignoreParseErrors) {
        if (expected > actual && !ignoreParseErrors) {
            throw new RuntimeException(
                    "Row length mismatch. "
                            + expected
                            + " fields expected but was "
                            + actual
                            + ".");
        }
    }

    @FunctionalInterface
    public interface ValueConverter extends Serializable {
        Object convert(JsonNode node) throws Exception;
    }
}
