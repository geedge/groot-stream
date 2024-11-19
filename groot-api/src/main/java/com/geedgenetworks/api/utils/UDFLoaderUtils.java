package com.geedgenetworks.api.utils;

import com.googlecode.aviator.Expression;
import com.googlecode.aviator.exception.ExpressionRuntimeException;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UDFLoaderUtils {
    public static Map<String, String> getClassReflect(List<String> plugins) {

        Map<String, String> classReflect = new HashMap<>();

        for (String classPath : plugins) {

            Class cls = null;
            try {
                cls = Class.forName(classPath);
                Method method = cls.getMethod("functionName");
                Object object = cls.newInstance();
                String result = (String) method.invoke(object);
                classReflect.put(result, classPath);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return classReflect;
    }

}
