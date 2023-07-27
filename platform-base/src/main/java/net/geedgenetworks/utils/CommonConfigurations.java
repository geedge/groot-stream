package net.geedgenetworks.utils;

import java.util.Properties;

public final class CommonConfigurations {

    private static Properties propService;

    public CommonConfigurations(Properties propService){
        this.propService = propService;
    }


    public static String getStringProperty(String key) {
        return propService.getProperty(key);
    }

    public static Integer getIntProperty(String key) {

        return Integer.parseInt(propService.getProperty(key));

    }

    public static Double getDoubleProperty(String key) {

        return Double.parseDouble(propService.getProperty(key));

    }

    public static Long getLongProperty(String key) {
        return Long.parseLong(propService.getProperty(key));

    }

    public static Boolean getBooleanProperty(Integer type, String key) {
        return "true".equals(propService.getProperty(key).toLowerCase().trim());
    }
}
