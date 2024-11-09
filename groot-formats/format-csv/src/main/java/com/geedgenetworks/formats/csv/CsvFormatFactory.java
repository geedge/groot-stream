package com.geedgenetworks.formats.csv;

import com.geedgenetworks.spi.table.connector.DecodingFormat;
import com.geedgenetworks.spi.table.connector.EncodingFormat;
import com.geedgenetworks.spi.table.factory.DecodingFormatFactory;
import com.geedgenetworks.spi.table.factory.EncodingFormatFactory;
import com.geedgenetworks.spi.table.factory.FactoryUtil;
import com.geedgenetworks.spi.table.factory.TableFactory;
import com.geedgenetworks.spi.table.type.*;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import static com.geedgenetworks.formats.csv.CsvFormatOptions.*;

public class CsvFormatFactory implements DecodingFormatFactory, EncodingFormatFactory {
    public static final String IDENTIFIER = "csv";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public DecodingFormat createDecodingFormat(TableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        validateFormatOptions(formatOptions);
        final boolean ignoreParseErrors = formatOptions.get(IGNORE_PARSE_ERRORS);
        return dataType -> {
            Preconditions.checkNotNull(dataType, "csv format require schema");
            CsvSchema csvSchema = getCsvSchema(dataType, formatOptions);
            return new CsvEventDeserializationSchema(dataType, csvSchema, ignoreParseErrors);
        };
    }

    @Override
    public EncodingFormat createEncodingFormat(TableFactory.Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        validateFormatOptions(formatOptions);
        return dataType -> {
            Preconditions.checkNotNull(dataType, "csv format require schema");
            CsvSchema csvSchema = getCsvSchema(dataType, formatOptions);
            return new CsvEventSerializationSchema(dataType, csvSchema);
        };
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FIELD_DELIMITER);
        options.add(DISABLE_QUOTE_CHARACTER);
        options.add(QUOTE_CHARACTER);
        options.add(ALLOW_COMMENTS);
        options.add(IGNORE_PARSE_ERRORS);
        options.add(ARRAY_ELEMENT_DELIMITER);
        options.add(ESCAPE_CHARACTER);
        options.add(NULL_LITERAL);
        return options;
    }

    static CsvSchema getCsvSchema(StructType dataType, ReadableConfig options){
        CsvSchema.Builder builder = convert(dataType).rebuild();

        options.getOptional(FIELD_DELIMITER)
                .map(delimiter -> StringEscapeUtils.unescapeJava(delimiter).charAt(0))
                .ifPresent(builder::setColumnSeparator);

        if (options.get(DISABLE_QUOTE_CHARACTER)) {
            builder.disableQuoteChar();
        } else {
            options.getOptional(QUOTE_CHARACTER)
                    .map(quote -> quote.charAt(0))
                    .ifPresent(builder::setQuoteChar);
        }

        options.getOptional(ARRAY_ELEMENT_DELIMITER).ifPresent(builder::setArrayElementSeparator);

        options.getOptional(ESCAPE_CHARACTER).map(quote -> quote.charAt(0)).ifPresent(builder::setEscapeChar);

        Optional.ofNullable(options.get(NULL_LITERAL)).ifPresent(builder::setNullValue);

        CsvSchema csvSchema = builder.build();

        return csvSchema;
    }

    public static CsvSchema convert(StructType schema) {
        CsvSchema.Builder builder = new CsvSchema.Builder();
        StructType.StructField[] fields = schema.fields;
        for (int i = 0; i < fields.length; i++) {
            String fieldName = fields[i].name;
            DataType dataType = fields[i].dataType;
            builder.addColumn(new CsvSchema.Column(i, fieldName, convertType(fieldName, dataType)));
        }
        return builder.build();
    }

    private static CsvSchema.ColumnType convertType(String fieldName, DataType dataType) {
        if (dataType instanceof StringType) {
            return CsvSchema.ColumnType.STRING;
        } else if (dataType instanceof IntegerType || dataType instanceof LongType || dataType instanceof FloatType || dataType instanceof DoubleType) {
            return CsvSchema.ColumnType.NUMBER;
        } else if (dataType instanceof BooleanType) {
            return CsvSchema.ColumnType.BOOLEAN;
        } else if (dataType instanceof ArrayType) {
            validateNestedField(fieldName, ((ArrayType) dataType).elementType);
            return CsvSchema.ColumnType.ARRAY;
        } else if (dataType instanceof StructType) {
            StructType rowType = (StructType) dataType;
            for (StructType.StructField field : rowType.fields) {
                validateNestedField(fieldName, field.dataType);
            }
            return CsvSchema.ColumnType.ARRAY;
        } else {
            throw new IllegalArgumentException(
                    "Unsupported type '" + dataType + "' for field '" + fieldName + "'.");
        }
    }

    private static void validateNestedField(String fieldName, DataType dataType) {
        if (!(dataType instanceof StringType || dataType instanceof IntegerType || dataType instanceof LongType ||
                dataType instanceof FloatType || dataType instanceof DoubleType || dataType instanceof BooleanType)) {
            throw new IllegalArgumentException(
                    "Only simple types are supported in the second level nesting of fields '"
                            + fieldName
                            + "' but was: "
                            + dataType);
        }
    }

    // ------------------------------------------------------------------------
    //  Validation
    // ------------------------------------------------------------------------

    static void validateFormatOptions(ReadableConfig tableOptions) {
        final boolean hasQuoteCharacter = tableOptions.getOptional(QUOTE_CHARACTER).isPresent();
        final boolean isDisabledQuoteCharacter = tableOptions.get(DISABLE_QUOTE_CHARACTER);
        if (isDisabledQuoteCharacter && hasQuoteCharacter) {
            throw new ValidationException(
                    "Format cannot define a quote character and disabled quote character at the same time.");
        }
        // Validate the option value must be a single char.
        validateCharacterVal(tableOptions, FIELD_DELIMITER, true);
        validateCharacterVal(tableOptions, ARRAY_ELEMENT_DELIMITER);
        validateCharacterVal(tableOptions, QUOTE_CHARACTER);
        validateCharacterVal(tableOptions, ESCAPE_CHARACTER);
    }

    /** Validates the option {@code option} value must be a Character. */
    private static void validateCharacterVal(
            ReadableConfig tableOptions, ConfigOption<String> option) {
        validateCharacterVal(tableOptions, option, false);
    }

    /**
     * Validates the option {@code option} value must be a Character.
     *
     * @param tableOptions the table options
     * @param option the config option
     * @param unescape whether to unescape the option value
     */
    private static void validateCharacterVal(
            ReadableConfig tableOptions, ConfigOption<String> option, boolean unescape) {
        if (tableOptions.getOptional(option).isPresent()) {
            final String value =
                    unescape
                            ? StringEscapeUtils.unescapeJava(tableOptions.get(option))
                            : tableOptions.get(option);
            if (value.length() != 1) {
                throw new ValidationException(
                        String.format(
                                "Option '%s.%s' must be a string with single character, but was: %s",
                                IDENTIFIER, option.key(), tableOptions.get(option)));
            }
        }
    }
}
