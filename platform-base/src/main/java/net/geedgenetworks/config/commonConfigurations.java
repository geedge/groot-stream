package net.geedgenetworks.config;

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
            propService.load(commonConfigurations.class.getClassLoader().getResourceAsStream("common.properties"));
        } catch (Exception e) {
            propService = null;
        }
    }
}
