package com.geedgenetworks.udf;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.geedgenetworks.pojo.GrootEvent;
import com.geedgenetworks.pojo.ProjectionFunction;

import java.util.Map;

public class ConditionFunction implements UdfFunction {


    private static final Log logger = LogFactory.get();
    private ProjectionFunction projection_function;
    public ConditionFunction(ProjectionFunction projection_function) {
        this.projection_function=projection_function;
    }
  /*  @Override
    public void udffunction(Map<String, Object> AuxiliaryFieldsAndValue) {


        Object result = null;
        try {
            String[] split = projection_function.getParameters().get("param").toString().split(",");
            if (split.length == 3) {
                String expression = split[0];
                Object resultA = isJsonValue(AuxiliaryFieldsAndValue, split[1]);
                Object resultB = isJsonValue(AuxiliaryFieldsAndValue, split[2]);
                if (expression.contains("!=")) {
                    String[] regexp = expression.split("!=");
                    Object direction = isJsonValue(AuxiliaryFieldsAndValue, regexp[0]);
                    if (direction instanceof Number) {
                        result = Integer.parseInt(direction.toString()) != Integer.parseInt(regexp[1]) ? resultA : resultB;
                    } else if (direction instanceof String) {
                        result = direction.equals(regexp[1]) ? resultA : resultB;
                    }
                } else {
                    String[] regexp = expression.split("=");
                    Object direction = isJsonValue(AuxiliaryFieldsAndValue, regexp[0]);
                    if (direction instanceof Number) {
                        result = Integer.parseInt(direction.toString()) == Integer.parseInt(regexp[1]) ? resultA : resultB;
                    } else if (direction instanceof String) {
                        result = direction.equals(regexp[1]) ? resultA : resultB;
                    }
                }
            }

            AuxiliaryFieldsAndValue.put(projection_function.getOutput_fields().get(0),result);
        } catch (RuntimeException e) {
            logger.error("IF function execution exception, exception information:" + e.getMessage());
        }
    }*/

    private static Object isJsonValue(Map<String, Object> AuxiliaryFieldsAndValue, String param) {
        if (param.contains("$.")) {
            return AuxiliaryFieldsAndValue.get(param.substring(2));
        } else {
            return param;
        }
    }

    @Override
    public void udffunction(GrootEvent grootEvent) {

    }
}
