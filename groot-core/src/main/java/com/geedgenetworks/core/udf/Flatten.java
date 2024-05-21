package com.geedgenetworks.core.udf;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.udf.UDF;
import com.geedgenetworks.common.udf.UDFContext;
import com.googlecode.aviator.Expression;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.util.*;


@Slf4j
public class Flatten implements UDF {

    private String prefix;
    private String delimiter;
    private int depth;
    private static Set HashSet;


    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {

        HashSet = new HashSet();
        HashSet.addAll(udfContext.getLookup_fields());
        prefix = udfContext.getParameters().getOrDefault("prefix", "").toString();
        depth = Integer.parseInt(udfContext.getParameters().getOrDefault("depth", "5").toString());
        delimiter = udfContext.getParameters().getOrDefault("delimiter", ".").toString();

    }


    @Override
    public Event evaluate(Event event) {


        Map<String, Object> flattenedMap = flatten(event.getExtractedFields(), prefix, depth, delimiter);
        event.setExtractedFields(flattenedMap);
        return event;
    }

    public static Map<String, Object> flatten(Map<String, Object> nestedMap, String prefix, int depth, String delimiter) {
        Map<String, Object> flattenedMap = new HashMap<>();
        nestedMap.forEach((key, value) -> {
            if (HashSet.isEmpty() || HashSet.contains(key)) {
                if (value instanceof List) {
                    flattenList(flattenedMap, (List<Map<String, Object>>) value, key, prefix, depth, delimiter);
                } else if (value instanceof Map && depth > 1) {
                    flattenMap(flattenedMap, (Map<String, Object>) value, key, prefix, depth - 1, delimiter);
                } else {
                    flattenedMap.put(prefix.isEmpty() ? key : prefix + delimiter + key, value);
                }
            } else {
                flattenedMap.put(key, value);
            }
        });
        return flattenedMap;
    }

    private static void flattenList(Map<String, Object> flattenedMap, List<Map<String, Object>> list, String topLevelKey, String prefix, int depth, String delimiter) {
        for (int i = 0; i < list.size(); i++) {
            Map<String, Object> map = list.get(i);
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                String key = "";
                if (!topLevelKey.isEmpty()) {
                    key = prefix.isEmpty() ? topLevelKey + delimiter + i + delimiter + entry.getKey() : prefix + delimiter + topLevelKey + delimiter + i + delimiter + entry.getKey();
                } else {
                    key = prefix.isEmpty() ? i + delimiter + entry.getKey() : prefix + delimiter + i + delimiter + entry.getKey();
                }
                if (entry.getValue() instanceof List) {
                    flattenList(flattenedMap, (List<Map<String, Object>>) entry.getValue(), "", key, depth, delimiter);
                } else if (entry.getValue() instanceof Map && depth > 1) {
                    flattenMap(flattenedMap, (Map<String, Object>) entry.getValue(), "", key, depth - 1, delimiter);
                } else {
                    flattenedMap.put(key, entry.getValue());
                }
            }
        }
    }

    private static void flattenMap(Map<String, Object> flattenedMap, Map<String, Object> map, String topLevelKey, String prefix, int depth, String delimiter) {
        map.forEach((key, value) -> {
            String flattenedKey = "";
            if (!topLevelKey.isEmpty()) {
                flattenedKey = prefix.isEmpty() ? topLevelKey + delimiter + key : prefix + delimiter + topLevelKey + delimiter + key;
            } else {
                flattenedKey = prefix.isEmpty() ? key : prefix + delimiter + key;
            }
            if (value instanceof List) {
                flattenList(flattenedMap, (List<Map<String, Object>>) value, "", flattenedKey, depth, delimiter);
            } else if (value instanceof Map && depth > 1) {
                flattenMap(flattenedMap, (Map<String, Object>) value, "", flattenedKey, depth - 1, delimiter);
            } else {
                flattenedMap.put(flattenedKey, value);
            }
        });
    }

    @Override
    public String functionName() {
        return "FLATTEN";
    }

    @Override
    public void close() {

    }

}
