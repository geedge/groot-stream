package com.geedgenetworks.config;

import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public final class GrootYamlParser {




    public static String getStringProperty(String key,Map<String, Object> yamlData) {

        return getYamlValue(yamlData,key,String.class);


    }

    public static Integer getIntProperty( String key,Map<String, Object> yamlData) {

        return getYamlValue(yamlData, key, Integer.class);

    }




    public static <T> T getYamlValue( Map<String, Object> data,String key, Class<T> valueType) {

        String[] keys = key.split("\\.");

        for (int i = 0; i < keys.length - 1; i++) {
            Object value = data.get(keys[i]);
            if (value == null || !(value instanceof Map)) {
                return null;
            }
            data = (Map<String, Object>) value;
        }

        return data.containsKey(keys[keys.length - 1]) ? valueType.cast(data.get(keys[keys.length - 1])) : null;
    }





    public static Map<String,String> getClassReflect(String key,Map<String, Object> yamlData)  {
        String value = (String) getValue(key,yamlData);
        String[] classArray =  value.split(" ");
        Map<String,String> classReflect = new HashMap<>();

        for(String classPath : classArray){

            Class cls = null;
            try {
                cls = Class.forName(classPath);
                Method method = cls.getMethod("functionName");
                Object object = cls.newInstance();
                String result = (String) method.invoke(object);
                classReflect.put(result,classPath);
                System.out.println("Returned Value: " + result);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        return classReflect;
    }



    public static Map<String, Object> loadYaml(InputStream input) throws Exception {
        // 读取YAML文件
        Map<String, Object> yamlData = new HashMap<>();
        Yaml yaml = new Yaml();
        yamlData = yaml.load(input);
        if(yamlData!=null){
            return yamlData;
        }
        else {
            throw new Exception("加载yaml配置文件失败");
        }

    }



    private static Object getValue(String key, Map<String, Object> data) {
        if (data.containsKey(key)) {
            return data.get(key);
        } else {
            for (Object value : data.values()) {
                if (value instanceof Map) {
                    Object result = getValue(key, (Map<String, Object>) value);
                    if (result != null) {
                        return result;
                    }
                }
            }
        }
        return null;
    }

    public static List<Object> getList(String key, Map<String, Object> yamlData) {
        Object value = getValue(key, yamlData);
        if (value instanceof List) {
            return (List<Object>) value;
        }
        return null;
    }

    public static Map<String, Object> getMap(String key,Map<String, Object> yamlData) {
        Object value = getValue(key,yamlData);
        if (value instanceof Map) {
            return (Map<String, Object>) value;
        }
        return null;
    }

    public static void main(String[] args) {
        System.out.println();

}}