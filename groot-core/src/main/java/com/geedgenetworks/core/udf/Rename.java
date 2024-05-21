package com.geedgenetworks.core.udf;

import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.udf.UDF;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.udf.UDFContext;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.AviatorEvaluatorInstance;
import com.googlecode.aviator.Expression;
import com.googlecode.aviator.Options;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.util.*;

public class Rename implements UDF {

    private static Expression compiledExp;
    private Set<String> parentFields;

    private Map<String, String> renameFields;
    private String renameExpression;

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {


        String parentFields = udfContext.getParameters().getOrDefault("parent_fields", "").toString();
        this.parentFields = new HashSet<>();
        if (!parentFields.isEmpty()) {
            String cleanedInput = parentFields.replaceAll("\\[|\\]|\\s+", "").replaceAll("\"", "");
            this.parentFields.addAll(List.of(cleanedInput.split(",")));
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


    public static Map<String, Object> rename(Map<String, Object> nestedMap, Set<String> parentFields, Map<String, String> renameFields, String renameExpression, String prefix) {
        Map<String, Object> renamedMap = new HashMap<>();
        nestedMap.forEach((key, value) -> {

            if (parentFields.isEmpty() || parentFields.contains(prefix + key)) {
                if (value instanceof Map) {
                    Map<String, Object> childMap = (Map<String, Object>) value;
                    childMap = renameMap(childMap, renameFields, renameExpression);
                    renamedMap.put(key, childMap);
                } else {
                    String newKey = key;
                    if (renameFields.containsKey(key)) {
                        newKey = renameFields.get(key);
                    }
                    if (!newKey.startsWith("__")) {
                        if (renameExpression != null && !renameExpression.isEmpty()) {
                            newKey = (String) compiledExp.execute(compiledExp.newEnv("key", newKey));
                        }
                    }
                    renamedMap.put(newKey, value);
                }
            } else {

                if (value instanceof Map) {
                    Map<String, Object> childMap = (Map<String, Object>) value;
                    childMap = rename(childMap, parentFields, renameFields, renameExpression, prefix + key + ".");
                    renamedMap.put(key, childMap);
                } else {
                    renamedMap.put(key, value);
                }
            }
        });

        return renamedMap;
    }

    private static Map<String, Object> renameMap(Map<String, Object> map, Map<String, String> renameFields, String renameExpression) {
        Map<String, Object> renamedMap = new HashMap<>();
        map.forEach((key, value) -> {
            String newKey = key;
            if (renameFields.containsKey(key)) {
                newKey = renameFields.get(key);
            }
            if (!newKey.startsWith("__")) {
                if (renameExpression != null && !renameExpression.isEmpty()) {
                    newKey = (String) compiledExp.execute(compiledExp.newEnv("key", newKey));
                }
            }
            renamedMap.put(newKey, value);
        });
        return renamedMap;
    }
}
