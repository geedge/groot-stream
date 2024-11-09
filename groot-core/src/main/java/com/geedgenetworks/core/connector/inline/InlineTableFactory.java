package com.geedgenetworks.core.connector.inline;

import com.alibaba.fastjson2.JSON;
import com.geedgenetworks.spi.table.connector.SourceProvider;
import com.geedgenetworks.spi.table.connector.DecodingFormat;
import com.geedgenetworks.spi.table.connector.SourceTableFactory;
import com.geedgenetworks.spi.table.factory.DecodingFormatFactory;
import com.geedgenetworks.spi.table.factory.FactoryUtil;
import com.geedgenetworks.spi.table.type.StructType;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.util.StringUtils;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

import static org.apache.flink.configuration.ConfigOptions.key;

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
        String type = config.get(TYPE);
        long intervalPerRow = config.get(INTERVAL_PER_ROW).toMillis();
        int repeatCount = config.get(REPEAT_COUNT);

        return new InlineSourceProvider(physicalDataType, decodingFormat, parseData(data, type), intervalPerRow, repeatCount);
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
        options.add(TYPE);
        options.add(INTERVAL_PER_ROW);
        options.add(REPEAT_COUNT);
        return options;
    }

    List<byte[]> parseData(String data, String type){
        List<byte[]> datas;
        if(JSON.isValidArray(data)){
            List<String> dataArray = JSON.parseArray(data, String.class);
            datas = new ArrayList<>(dataArray.size());
            for (int i = 0; i < dataArray.size(); i++) {
                datas.add(getDataBytes(dataArray.get(i), type));
            }
        }else{
            datas = new ArrayList<>(1);
            datas.add(getDataBytes(data, type));
        }
        return datas;
    }

    byte[] getDataBytes(String data, String type){
        if(DATA_TYPE_STRING.equalsIgnoreCase(type)){
            return data.getBytes(StandardCharsets.UTF_8);
        } else if (DATA_TYPE_HEX.equalsIgnoreCase(type)){
            return StringUtils.hexStringToByte(data);
        } else if (DATA_TYPE_BASE64.equalsIgnoreCase(type)) {
            return Base64.getDecoder().decode(data.getBytes(StandardCharsets.UTF_8));
        }else{
            throw new IllegalArgumentException("Unsupported type:" + type);
        }
    }

    final static String DATA_TYPE_STRING = "string";
    final static String DATA_TYPE_HEX = "hex";
    final static String DATA_TYPE_BASE64 = "base64";
    public static final ConfigOption<String> DATA =
            ConfigOptions.key("data")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("inline source的输入数据");
    public static final ConfigOption<String> TYPE =
            ConfigOptions.key("type")
                    .stringType()
                    .defaultValue(DATA_TYPE_STRING)
                    .withDescription("数据类型:string(UTF8字符串),hex(十六进制编码),base64(base64编码)");
    public static final ConfigOption<Duration> INTERVAL_PER_ROW =
            ConfigOptions.key("interval.per.row")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription("sleep interval per row to control the emit rate.");
    public static final ConfigOption<Integer> REPEAT_COUNT =
            key("repeat.count")
                    .intType()
                    .defaultValue(-1)
                    .withDescription("repeat emit data count. By default, the source is unbounded.");
}
