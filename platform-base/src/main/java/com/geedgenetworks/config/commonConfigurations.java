package com.geedgenetworks.config;

import org.yaml.snakeyaml.Yaml;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


/**
 * @author Administrator
 */

public final class commonConfigurations {

    private static Properties propService = new Properties();

    public static Map<String,String> getHashTableProperty(String key) {

        Map<String,String> map = new HashMap<>();


            String[] keyarray = propService.getProperty(key).split(",");
            for(String k :keyarray){

                if(k!=null &&  !"".equals(k.trim())){
                    map.put(k,"");
                }

            }

        return map;
    }

    public static String getStringProperty(String key) {

            return propService.getProperty(key);


    }

    public static Integer getIntProperty( String key) {

            return Integer.parseInt(propService.getProperty(key));

    }

    public static Long getLongProperty(String key) {
            return Long.parseLong(propService.getProperty(key));

    }

    public static Boolean getBooleanProperty(String key) {
            return "true".equals(propService.getProperty(key).toLowerCase().trim());
    }

    static {
        try {
          //  propService.load(commonConfigurations.class.getClassLoader().getResourceAsStream("common.properties"));



            // 加载 YAML 文件
            Yaml yaml = new Yaml();

            Map<String, Object> data = yaml.load(YamlParser.class.getClassLoader().getResourceAsStream("business.yaml"));


            // 解析YAML文件并获取根对

            // 转换为 Properties 格式数据
            for (Map.Entry<String, Object> entry : data.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                if (value instanceof Map) {
                    Map<String, Object> subData = (Map<String, Object>) value;
                    for (Map.Entry<String, Object> subEntry : subData.entrySet()) {
                        String subKey = key + "." + subEntry.getKey();
                        Object subValue = subEntry.getValue();
                        propService.put(subKey, subValue != null ? subValue.toString() : "");
                    }
                } else {
                    propService.put(key, value != null ? value.toString() : "");
                }
            }





        } catch (Exception e) {
            propService = null;
        }
    }
}
