package com.geedgenetworks.core.udf.udtf;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.udf.TableFunction;
import com.geedgenetworks.common.udf.UDFContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.util.*;


@Slf4j
public class Unroll implements TableFunction {

    private String lookupFieldName;
    private String outputFieldName;
    private String regex;


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
            regex="";
        }
        else {
            this.regex=udfContext.getParameters().getOrDefault("regex", "").toString().trim();
        }
    }

    @Override
    public List<Event> evaluate(Event event) {
        try {
            Object valueObj = event.getExtractedFields().get(lookupFieldName);
            if (valueObj!=null) {
                if(regex.isEmpty()){
                    if (valueObj instanceof List  ) {
                        return parseList(valueObj, event);
                    } else if(valueObj instanceof Object[]){
                        return parseArray(valueObj, event);
                    }else {
                        log.error("Invalid unroll ! Object is not instance of list or array. expression=" + regex);
                    }
                }
                else {
                    if (valueObj instanceof String) {
                        String[] array =((String) valueObj).split(regex);
                        return parseArray(array, event);
                    }else {
                        log.error("Invalid unroll ! Object is not instance of String. expression=" + regex);
                    }
                }
            }
        }catch (Exception e) {
            log.error("Invalid parseObject ! expression=" +regex + " Exception :" + e.getMessage());
        }
        return Collections.singletonList(event);
    }

    private List<Event> parseList(Object object,Event event) {
        List list = (List) object;
        List<Event> eventList = new ArrayList<>();
        for (Object obj : list) {
            Event newEvent = new Event();
            newEvent.setExtractedFields(new HashMap<>());
            newEvent.getExtractedFields().putAll(event.getExtractedFields());
            newEvent.getExtractedFields().remove(lookupFieldName);
            newEvent.getExtractedFields().put(outputFieldName, obj);
            eventList.add(newEvent);
        }
        return eventList;
    }
    private List<Event> parseArray(Object object, Event event) {
        List<Event> eventList = new ArrayList<>();
        Object[] objects = (Object[]) object;
        for (Object obj : objects) {
            Event newEvent = new Event();
            newEvent.setExtractedFields(new HashMap<>());
            newEvent.getExtractedFields().putAll(event.getExtractedFields());
            newEvent.getExtractedFields().remove(lookupFieldName);
            newEvent.getExtractedFields().put(outputFieldName, obj);
            eventList.add(newEvent);
        }
        return eventList;
    }
    @Override
    public String functionName() {
        return "UNROLL";
    }

    @Override
    public void close() {

    }

}
