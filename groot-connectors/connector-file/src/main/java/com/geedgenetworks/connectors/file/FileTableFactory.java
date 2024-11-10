package com.geedgenetworks.connectors.file;

import com.geedgenetworks.spi.table.connector.DecodingFormat;
import com.geedgenetworks.spi.source.SourceProvider;
import com.geedgenetworks.spi.source.SourceTableFactory;
import com.geedgenetworks.spi.table.factory.DecodingFormatFactory;
import com.geedgenetworks.spi.table.factory.FactoryUtil;
import com.geedgenetworks.spi.table.type.StructType;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;

import java.util.HashSet;
import java.util.Set;

import static com.geedgenetworks.connectors.file.FileConnectorOptions.*;

public class FileTableFactory implements SourceTableFactory {
    public static final String IDENTIFIER = "file";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public SourceProvider getSourceProvider(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        DecodingFormat decodingFormat = helper.discoverDecodingFormat(DecodingFormatFactory.class, FactoryUtil.FORMAT);
        helper.validate();

        StructType physicalDataType = context.getPhysicalDataType();
        ReadableConfig config = context.getConfiguration();

        String path = config.get(PATH).trim();
        boolean readLocalFileInClient = config.get(READ_LOCAL_FILE_IN_CLIENT);
        int rowsPerSecond = config.get(ROWS_PER_SECOND);
        long numberOfRows = config.get(NUMBER_OF_ROWS);
        long millisPerRow = config.get(MILLIS_PER_ROW);

        return new FileSourceProvider(physicalDataType, decodingFormat.createRuntimeDecoder(physicalDataType), path, readLocalFileInClient, rowsPerSecond, numberOfRows, millisPerRow);
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PATH);
        options.add(FactoryUtil.FORMAT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(ROWS_PER_SECOND);
        options.add(NUMBER_OF_ROWS);
        options.add(MILLIS_PER_ROW);
        options.add(READ_LOCAL_FILE_IN_CLIENT);
        return options;
    }
}
