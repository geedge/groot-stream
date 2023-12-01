package com.geedgenetworks.core.connector.inline;

import com.geedgenetworks.core.connector.format.DecodingFormat;
import com.geedgenetworks.core.connector.source.SourceProvider;
import com.geedgenetworks.core.factories.DecodingFormatFactory;
import com.geedgenetworks.core.factories.FactoryUtil;
import com.geedgenetworks.core.factories.SourceTableFactory;
import com.geedgenetworks.core.types.StructType;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

/**
 * 用于测试的source，用于简单测试format、function等
 */
public class InlineTableFactory implements SourceTableFactory {
    public static final String IDENTIFIER = "inline";
    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public SourceProvider getSourceProvider(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        // 获取DecodingFormat
        DecodingFormat decodingFormat = helper.discoverDecodingFormat(DecodingFormatFactory.class, FactoryUtil.FORMAT);

        helper.validate(); // 校验参数

        StructType physicalDataType = context.getPhysicalDataType(); // 列类型
        ReadableConfig config = context.getConfiguration();

        String data = config.get(DATA);

        return new InlineSourceProvider(physicalDataType, decodingFormat, data.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DATA);
        options.add(FactoryUtil.FORMAT);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        return options;
    }

    public static final ConfigOption<String> DATA =
            ConfigOptions.key("data")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("inline source的输入数据");
}
