package com.geedgenetworks.core.udf;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.api.common.udf.ScalarFunction;
import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.connector.event.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.util.*;


@Slf4j
public class Flatten implements ScalarFunction {

    private String prefix;
    private String delimiter;
    private int depth;
    private Set<String> flattenKeys;
    private Set<String> jsonStringKeys;

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {
        if(udfContext.getParameters()==null){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Missing required parameters");
        }
        prefix = udfContext.getParameters().getOrDefault("prefix", "").toString();
        delimiter = udfContext.getParameters().getOrDefault("delimiter", ".").toString();
        flattenKeys = new HashSet<>();
        for (String key : udfContext.getLookupFields()) {
            this.flattenKeys.add(prefix.isEmpty() ? key : prefix + delimiter + key);
        }
        depth = Integer.parseInt(udfContext.getParameters().getOrDefault("depth", "5").toString());
        String jsonStringKeys = udfContext.getParameters().getOrDefault("json_string_keys", "").toString();
        this.jsonStringKeys = new HashSet<>();
        if (!jsonStringKeys.isEmpty()) {
            List<String> jsonStringKeyList;
            try {
                jsonStringKeyList = JSONArray.parseArray(jsonStringKeys, String.class);
            }
            catch (Exception e){
                throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The param of json_string_keys is illegal");
            }
            for (String key : jsonStringKeyList) {
                this.jsonStringKeys.add(prefix.isEmpty() ? key : prefix + delimiter + key);
            }
        }
    }

    @Override
    public Event evaluate(Event event) {
        Map<String, Object> flattenedMap = flatten(event.getExtractedFields(), prefix, depth, delimiter);
        event.setExtractedFields(flattenedMap);
        return event;
    }

    private  Map<String, Object> flatten(Map<String, Object> nestedMap, String prefix, int depth, String delimiter) {
        Map<String, Object> flattenedMap = new HashMap<>();
        nestedMap.forEach((key, value) -> {
            String fullkey = prefix.isEmpty() ? key : prefix + delimiter + key;
            if (flattenKeys.isEmpty() || flattenKeys.contains(fullkey)) {
                flattenValue(flattenedMap, fullkey, value, depth, delimiter);
            } else {
                flattenedMap.put(key, value);
            }
        });
        return flattenedMap;
    }

    private void flattenString(Map<String, Object> flattenedMap, String value,String fullkey, int depth, String delimiter) {
        try {
            Object obj = JSONObject.parseObject(value, Object.class);
            if(obj instanceof List){
                flattenList(flattenedMap, (List<Map<String, Object>>) obj, fullkey, depth, delimiter);
            }
            else if(obj instanceof Map){
                flattenMap(flattenedMap, (Map<String, Object>) obj, fullkey, depth, delimiter);
            }
           else {
                flattenedMap.put(fullkey, value);
           }
        }catch (Exception e) {
            flattenedMap.put(fullkey, value);
        }
    }

    private void flattenList(Map<String, Object> flattenedMap, List<Map<String, Object>> list, String prefixKey, int depth, String delimiter) {
        for (int i = 0; i < list.size(); i++) {
            Map<String, Object> map = list.get(i);
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                String key=prefixKey+ delimiter + i + delimiter + entry.getKey();
                flattenValue(flattenedMap, key, entry.getValue(), depth, delimiter);
            }
        }
    }

    private void flattenMap(Map<String, Object> flattenedMap, Map<String, Object> map,String prefixKey, int depth, String delimiter) {
        map.forEach((key, value) -> {
            String fullkey = prefixKey + delimiter + key;
            flattenValue(flattenedMap, fullkey, value, depth, delimiter);
        });
    }


    private void flattenValue(Map<String, Object> flattenedMap, String fullkey,Object value, int depth, String delimiter) {

        if (jsonStringKeys.isEmpty() || !jsonStringKeys.contains(fullkey)) {
            if (value instanceof List) {
                flattenList(flattenedMap, (List<Map<String, Object>>) value, fullkey, depth, delimiter);
            } else if (value instanceof Map && depth > 1) {
                flattenMap(flattenedMap, (Map<String, Object>) value, fullkey, depth - 1, delimiter);
            } else {
                flattenedMap.put(fullkey, value);
            }
        } else {
            if (value instanceof String && depth > 1) {
                flattenString(flattenedMap, (String) value,fullkey, depth - 1, delimiter);
            } else {
                flattenedMap.put(fullkey, value);
            }
        }
    }



    @Override
    public String functionName() {
        return "FLATTEN";
    }

    @Override
    public void close() {

    }

}
