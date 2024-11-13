package com.geedgenetworks.core.udf;

import com.alibaba.fastjson2.JSONArray;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.api.common.udf.ScalarFunction;
import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.event.Event;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.AviatorEvaluatorInstance;
import com.googlecode.aviator.Expression;
import com.googlecode.aviator.Options;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.util.*;
@Slf4j
public class Rename implements ScalarFunction {

    private static Expression compiledExp;
    private Set<String> parentFields;

    private Map<String, String> renameFields;
    private String renameExpression;

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {

        if(udfContext.getParameters()==null ){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Missing required parameters");
        }
        String parentFields = udfContext.getParameters().getOrDefault("parent_fields", "").toString();
        this.parentFields = new HashSet<>();
        if (!parentFields.isEmpty()) {
            List<String> parentFieldList;
            try {
                parentFieldList = JSONArray.parseArray(parentFields, String.class);
            }
            catch (Exception e){
                throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The param of parent_fields is illegal");
            }
            this.parentFields.addAll(parentFieldList);
        }
        renameFields = (Map<String, String>) udfContext.getParameters().getOrDefault("rename_fields", new HashMap<>());
        renameExpression = udfContext.getParameters().getOrDefault("rename_expression", "").toString();
        if (!renameExpression.isEmpty()) {
            AviatorEvaluatorInstance instance = AviatorEvaluator.getInstance();
            instance.setCachedExpressionByDefault(true);
            instance.setOption(Options.OPTIMIZE_LEVEL, AviatorEvaluator.EVAL);
            instance.setFunctionMissing(null);
            compiledExp = instance.compile(renameExpression, true);
        }
    }

    @Override
    public Event evaluate(Event event) {
        Map<String, Object> renamedMap = rename(event.getExtractedFields(), parentFields, renameFields, renameExpression, "");
        event.setExtractedFields(renamedMap);
        return event;
    }

    @Override
    public String functionName() {
        return "RENAME";
    }

    @Override
    public void close() {

    }


    private Map<String, Object> rename(Map<String, Object> nestedMap, Set<String> parentFields, Map<String, String> renameFields, String renameExpression, String prefix) {
        Map<String, Object> renamedMap = new HashMap<>();
        nestedMap.forEach((key, value) -> {

            if (parentFields.contains(prefix + key)) {
                if (value instanceof Map) {
                    Map<String, Object> childMap = (Map<String, Object>) value;
                    childMap = rename(childMap, parentFields, renameFields, renameExpression,prefix+key+".");
                    childMap = renameMap(childMap, renameFields, renameExpression);
                    value = childMap;
                }
                key = renameKey(key, renameFields, renameExpression);
            }
            else {
                if (value instanceof Map) {
                    Map<String, Object> childMap = (Map<String, Object>) value;
                    childMap = rename(childMap, parentFields, renameFields, renameExpression,prefix+key+".");
                    value = childMap;
                }
            }
            if (prefix.isEmpty()) {//判断是否顶层
                key = renameKey(key, renameFields, renameExpression);
            }
            renamedMap.put(key, value);
        });

        return renamedMap;
    }

   private Map<String, Object> renameMap(Map<String, Object> nestedMap, Map<String, String> renameFields, String renameExpression) {
        Map<String, Object> renamedMap = new HashMap<>();
        nestedMap.forEach((key, value) -> {
            key = renameKey(key, renameFields, renameExpression);
            renamedMap.put(key, value);
        });
        return renamedMap;
    }


    private String renameKey(String key,Map<String, String> renameFields, String renameExpression) {

            String newKey = key;
            if (renameFields.containsKey(key)) {
                newKey = renameFields.get(key);
            }
            if (!newKey.startsWith("__")) {
                if (renameExpression != null && !renameExpression.isEmpty()) {
                    try {
                        newKey = (String) compiledExp.execute(compiledExp.newEnv("key", newKey));
                    } catch (Exception e) {
                        log.error("Error evaluating rename expression: " + renameExpression);
                    }
                }
            }
        return newKey;
    }
}
