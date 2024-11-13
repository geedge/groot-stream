package com.geedgenetworks.connectors.mock.faker;

import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONReader;
import com.geedgenetworks.connectors.mock.faker.ObjectFaker.FieldFaker;
import com.geedgenetworks.connectors.mock.faker.NumberFaker.*;
import com.geedgenetworks.connectors.mock.faker.StringFaker.*;
import com.geedgenetworks.connectors.mock.faker.TimestampFaker.*;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.geedgenetworks.api.connector.type.DataType;
import com.geedgenetworks.api.connector.type.Types;
import org.apache.flink.util.Preconditions;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class FakerUtils {

    public static ObjectFaker parseObjectFakerFromJson(String json) {
        JSONArray jsonArray = JSON.parseArray(json, JSONReader.Feature.UseBigDecimalForDoubles);
        return parseObjectFaker(jsonArray);
    }

    private static Faker<?> parseFaker(JSONObject obj) {
        String type = obj.getString("type");
        Preconditions.checkNotNull(type, "type is required");
        type = type.trim();

        if ("Number".equalsIgnoreCase(type)) {
            return wrapFaker(parseNumberFaker(obj), obj);
        } else if ("Sequence".equalsIgnoreCase(type)) {
            return wrapFaker(parseSequenceFaker(obj), obj);
        } else if ("UniqueSequence".equalsIgnoreCase(type)) {
            return wrapFaker(parseUniqueSequenceFaker(obj), obj);
        } else if ("String".equalsIgnoreCase(type)) {
            return wrapFaker(parseStringFaker(obj), obj);
        } else if ("Timestamp".equalsIgnoreCase(type)) {
            return wrapFaker(parseTimestampFaker(obj), obj);
        } else if ("FormatTimestamp".equalsIgnoreCase(type)) {
            return wrapFaker(parseFormatTimestampFaker(obj), obj);
        } else if ("IPv4".equalsIgnoreCase(type)) {
            return wrapFaker(parseIPv4Faker(obj), obj);
        } else if ("Expression".equalsIgnoreCase(type)) {
            return wrapFaker(parseExpressionFaker(obj), obj);
        } else if ("Hlld".equalsIgnoreCase(type)) {
            return wrapFaker(parseHlldFaker(obj), obj);
        } else if ("HdrHistogram".equalsIgnoreCase(type)) {
            return wrapFaker(parseHdrHistogramFaker(obj), obj);
        } else if ("Object".equalsIgnoreCase(type)) {
            return wrapFaker(parseObjectFaker(obj.getJSONArray("fields")), obj);
        } else if ("Union".equalsIgnoreCase(type)) {
            return wrapFaker(parseUnionFaker(obj), obj);
        } else if ("Eval".equalsIgnoreCase(type)) {
            return parseEvalFaker(obj);
        }

        throw new UnsupportedOperationException("not support type:" + type);
    }

    private static Faker<?> wrapFaker(Faker<?> faker, JSONObject obj) {
        if(obj.getBooleanValue("array", false)){
            faker = new ArrayFaker((Faker<Object>) faker, obj.getIntValue("arrayLenMin", 0), obj.getIntValue("arrayLenMax", 5));
        }
        return NullAbleFaker.wrap(faker, obj.getDoubleValue("nullRate"));
    }

    private static ObjectFaker parseObjectFaker(JSONArray fieldJsonArray) {
        return new ObjectFaker(parseObjectFieldFakers(fieldJsonArray));
    }

    private static FieldFaker[] parseObjectFieldFakers(JSONArray fieldJsonArray) {
        FieldFaker[] fields = new FieldFaker[fieldJsonArray.size()];

        for (int i = 0; i < fieldJsonArray.size(); i++) {
            JSONObject jsonObject = fieldJsonArray.getJSONObject(i);
            String name = jsonObject.getString("name");
            fields[i] = new FieldFaker(name, parseFaker(jsonObject));
        }

        return  fields;
    }


    private static UnionFaker parseUnionFaker(JSONObject obj) {
        JSONArray fieldsJsonArray = obj.getJSONArray("unionFields");
        boolean random = obj.getBooleanValue("random", true);
        UnionFaker.FieldsFaker[] fieldsFakers = new UnionFaker.FieldsFaker[fieldsJsonArray.size()];

        for (int i = 0; i < fieldsJsonArray.size(); i++) {
            JSONObject jsonObject = fieldsJsonArray.getJSONObject(i);
            int weight = jsonObject.getIntValue("weight", 1);
            Preconditions.checkArgument(weight >= 0 && weight < 10000000);
            FieldFaker[] fields = parseObjectFieldFakers(jsonObject.getJSONArray("fields"));
            fieldsFakers[i] = new UnionFaker.FieldsFaker(fields, weight);
        }

        return new UnionFaker(fieldsFakers, random);
    }

    private static Faker<?> parseEvalFaker(JSONObject obj) {
        String expression = obj.getString("expression");
        Preconditions.checkNotNull(expression);
        return new EvalFaker(expression);
    }

    private static Faker<?> parseExpressionFaker(JSONObject obj) {
        String expression = obj.getString("expression");
        Preconditions.checkNotNull(expression);
        return new ExpressionFaker(expression);
    }

    private static Faker<?> parseHlldFaker(JSONObject obj) {
        long itemCount = obj.getLongValue("itemCount", 1000000L);
        int batchCount = obj.getIntValue("batchCount", 10000);
        int precision = obj.getIntValue("precision", 12);
        return new HlldFaker(itemCount, batchCount, precision);
    }

    private static Faker<?> parseHdrHistogramFaker(JSONObject obj) {
        int max = obj.getIntValue("max", 100000);
        int batchCount = obj.getIntValue("batchCount", 1000);
        int numberOfSignificantValueDigits = obj.getIntValue("numberOfSignificantValueDigits", 1);
        return new HdrHistogramFaker(max, batchCount, numberOfSignificantValueDigits);
    }

    private static Faker<?> parseIPv4Faker(JSONObject obj) {
        String start = obj.getString("start");
        String end = obj.getString("end");
        if(start == null){
            start = "0.0.0.0";
        }
        if(end == null){
            start = "255.255.255.255";
        }
        return new IPv4Faker(IPv4Faker.ipv4ToLong(start), IPv4Faker.ipv4ToLong(end) + 1);
    }

    private static Faker<?> parseFormatTimestampFaker(JSONObject obj) {
        String format = obj.getString("format");
        boolean utc = obj.getBooleanValue("utc", false);
        if(format == null){
            format = FormatTimestamp.NORM_DATETIME_PATTERN;
        }
        return new FormatTimestamp(format, utc);
    }

    private static Faker<?> parseTimestampFaker(JSONObject obj) {
        String unit = obj.getString("unit");
        if("millis".equals(unit)){
            return new Timestamp();
        }else{
            return new UnixTimestamp();
        }
    }

    private static Faker<?> parseUniqueSequenceFaker(JSONObject obj) {
        long start = obj.getLongValue("start", 0L);
        return new UniqueSequenceFaker(start);
    }

    private static Faker<?> parseSequenceFaker(JSONObject obj) {
        long start = obj.getLongValue("start", 0L);
        long step = obj.getLongValue("step", 1L);
        int batch = obj.getIntValue("batch", 1);
        return new SequenceFaker(start, step, batch);
    }

    private static Faker<?> parseStringFaker(JSONObject obj) {
        String regex = obj.getString("regex");
        JSONArray options = obj.getJSONArray("options");
        boolean random = obj.getBooleanValue("random", true);

        if (options != null && options.size() > 0) {
            return new OptionString(options.stream().map(x -> x == null ? null : x.toString()).toArray(String[]::new), random);
        }else{
            if(regex == null){
                regex = "[a-zA-Z]{0,5}";
            }
            return new RegexString(regex);
        }
    }

    private static Faker<?> parseNumberFaker(JSONObject obj) {
        Number start = (Number) obj.get("min");
        Number end = (Number) obj.get("max");
        JSONArray options = obj.getJSONArray("options");
        boolean random = obj.getBooleanValue("random", true);

        DataType dataType;
        if (options != null && options.size() > 0) {
            dataType = getNumberDataType(options.stream().map(x -> (Number) x).collect(Collectors.toList()));
            if (dataType.equals(Types.INT)) {
                return new OptionIntNumber(options.stream().map(x -> x == null ? null : ((Number) x).intValue()).toArray(Integer[]::new), random);
            } else if (dataType.equals(Types.BIGINT)) {
                return new OptionLongNumber(options.stream().map(x -> x == null ? null : ((Number) x).longValue()).toArray(Long[]::new), random);
            } else {
                return new OptionDoubleNumber(options.stream().map(x -> x == null ? null : ((Number) x).doubleValue()).toArray(Double[]::new), random);
            }
        } else {
            if(start == null){
                start = 0;
            }
            if(end == null){
                end = Integer.MAX_VALUE;
            }
            Preconditions.checkArgument(end.doubleValue() > start.doubleValue());
            dataType = getNumberDataType(Arrays.asList(start, end));
            if (dataType.equals(Types.INT)) {
                return new RangeIntNumber(start.intValue(), end.intValue(), random);
            } else if (dataType.equals(Types.BIGINT)) {
                return new RangeLongNumber(start.longValue(), end.longValue(), random);
            } else {
                return new RangeDoubleNumber(start.doubleValue(), end.doubleValue());
            }
        }
    }

    private static DataType getNumberDataType(List<Number> list) {
        DataType dataType = Types.INT;

        for (Number number : list) {
            if (number == null) {
                continue;
            }

            if (number instanceof Short || number instanceof Integer) {
                continue;
            }

            if (number instanceof Long) {
                if (!dataType.equals(Types.DOUBLE)) {
                    dataType = Types.BIGINT;
                }
                continue;
            }

            if (number instanceof Float || number instanceof Double || number instanceof BigDecimal) {
                dataType = Types.DOUBLE;
                continue;
            }

            throw new IllegalArgumentException(number.toString());
        }

        return dataType;
    }

}
