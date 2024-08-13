package com.geedgenetworks.core.udf.udtf;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.udf.TableFunction;
import com.geedgenetworks.common.udf.UDFContext;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.AviatorEvaluatorInstance;
import com.googlecode.aviator.Expression;
import com.googlecode.aviator.Options;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.util.*;


@Slf4j
public class UnRoll implements TableFunction {

    private String lookupFieldName;
    private String outputFieldName;
    private Expression compiledExp;
    private String expression;
    private String outputFieldType;


    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {
        if(udfContext.getLookup_fields()==null || udfContext.getOutput_fields()==null){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Missing required parameters");
        }
        this.lookupFieldName = udfContext.getLookup_fields().get(0);
        this.outputFieldName = udfContext.getOutput_fields().get(0);
        this.outputFieldType="object";
        if(udfContext.getParameters()==null ){
            expression="";
        }
        else {
            this.outputFieldType=udfContext.getParameters().getOrDefault("output_field_type", "object").toString().trim();
            expression=udfContext.getParameters().getOrDefault("path", "").toString().trim();
        }
        if(!expression.isEmpty()){
            AviatorEvaluatorInstance instance = AviatorEvaluator.getInstance();
            instance.setCachedExpressionByDefault(true);
            instance.setOption(Options.OPTIMIZE_LEVEL, AviatorEvaluator.EVAL);
            instance.setFunctionMissing(null);
            compiledExp = instance.compile("event."+expression, true);
        }

    }

    @Override
    public List<Event> evaluate(Event event) {
        try {
            if(event.getExtractedFields().containsKey(lookupFieldName)){
                Object object;
                if(event.getExtractedFields().get(lookupFieldName) instanceof String){
                    object = JSONObject.parseObject((String) event.getExtractedFields().get(lookupFieldName), Object.class);
                }
                else {
                    object = event.getExtractedFields().get(lookupFieldName);
                }
                try {
                    if(compiledExp!=null){
                        Object obj =  compiledExp.execute(compiledExp.newEnv("event", object));
                        if(obj instanceof List) {
                            return parseList(obj,event);
                        }
                        else if(obj instanceof String){
                            object = JSONObject.parseObject((String)obj, Object.class);
                            if(object instanceof List){
                                return parseList(object,event);
                            }
                            else {
                                log.error("Invalid unroll ! Object is not instance of list. expression=" +expression);
                            }
                        }else {
                            log.error("Invalid unroll ! Object is not instance of String or List. expression=" +expression);
                        }
                    }
                    else {
                        if(object instanceof List){
                            return parseList(object,event);
                        }
                        else {
                            log.error("Invalid unroll ! Object is not instance of list. ");
                        }
                    }
                }catch (Exception e) {
                    log.error("Invalid unroll ! expression=" +expression + " Exception :" + e.getMessage());
                }
            }
        }catch (Exception e) {
            log.error("Invalid parseObject ! expression=" +expression + " Exception :" + e.getMessage());
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
            if("string".equals(outputFieldType)) {
                String jsonString = JSON.toJSONString(obj);
                newEvent.getExtractedFields().put(outputFieldName, jsonString);
            }
            else {
                newEvent.getExtractedFields().put(outputFieldName, obj);
            }
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
