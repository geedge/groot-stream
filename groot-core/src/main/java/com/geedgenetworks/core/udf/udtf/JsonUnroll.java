package com.geedgenetworks.core.udf.udtf;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.utils.JsonPathUtil;
import com.geedgenetworks.api.common.udf.TableFunction;
import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.event.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;


@Slf4j
public class JsonUnroll implements TableFunction {

    private String lookupFieldName;
    private String outputFieldName;
    private String path;
    private String new_Path;


    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {
        if(udfContext.getLookupFields()==null ){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Missing required parameters");
        }
        this.lookupFieldName  = udfContext.getLookupFields().get(0);
        if(udfContext.getOutputFields()!=null && !udfContext.getOutputFields().isEmpty()) {
            this.outputFieldName = udfContext.getOutputFields().get(0);
        }
        else {
            outputFieldName = lookupFieldName;
        }
        if(udfContext.getParameters()==null ){
            path="";
            new_Path="";
        }
        else {
            path=udfContext.getParameters().getOrDefault("path", "").toString().trim();
            new_Path=udfContext.getParameters().getOrDefault("new_path", path).toString().trim();
        }
    }

    @Override
    public List<Event> evaluate(Event event) {
        try {
            Object valueObj = event.getExtractedFields().get(lookupFieldName);
            if (valueObj!=null) {
                try {
                    if(path.isEmpty()){
                        JSONArray jsonArray = JSONArray.parseArray(valueObj.toString());
                        return parseList(jsonArray,event);
                    }else {
                        JSONObject jsonObject = JSONObject.parseObject(valueObj.toString());
                        Object obj = JsonPathUtil.get(jsonObject,path);
                        if(obj instanceof List || obj instanceof Array) {
                            List list = (List) obj;
                            List<Event> eventList = new ArrayList<>();
                            for (Object o : list) {
                                JSONObject newJsonObject = new JSONObject();
                                newJsonObject.putAll(jsonObject);
                                JsonPathUtil.remove(newJsonObject,path);
                                JsonPathUtil.set(newJsonObject,new_Path,o);
                                String jsonString = JSON.toJSONString(newJsonObject);
                                Event newEvent = new Event();
                                newEvent.setExtractedFields(new HashMap<>());
                                newEvent.getExtractedFields().putAll(event.getExtractedFields());
                                newEvent.getExtractedFields().remove(lookupFieldName);
                                newEvent.getExtractedFields().put(outputFieldName, jsonString);
                                eventList.add(newEvent);
                            }
                            return eventList;
                        }
                        else {
                            log.error("Invalid unroll ! expression=" +path + " Exception :" + " expression should return a list or array");
                        }
                    }

                }catch (Exception e) {
                    log.error("Invalid unroll ! expression=" +path + " Exception :" + e.getMessage());
                }
            }
        }catch (Exception e) {
            log.error("Invalid parseObject ! expression=" +path + " Exception :" + e.getMessage());
        }
        return Collections.singletonList(event);
    }

    private List<Event> parseList(Object object, Event event) {
        List list = (List) object;
        List<Event> eventList = new ArrayList<>();
        for (Object obj : list) {
            Event newEvent = new Event();
            newEvent.setExtractedFields(new HashMap<>());
            newEvent.getExtractedFields().putAll(event.getExtractedFields());
            newEvent.getExtractedFields().remove(lookupFieldName);
            newEvent.getExtractedFields().put(outputFieldName, JSON.toJSONString(obj));
            eventList.add(newEvent);
        }
        return eventList;
    }

    @Override
    public String functionName() {
        return "JSON_UNROLL";
    }

    @Override
    public void close() {

    }

}
