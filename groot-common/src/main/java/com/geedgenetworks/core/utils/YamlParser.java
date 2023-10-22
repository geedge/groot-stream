package com.geedgenetworks.core.utils;

import org.yaml.snakeyaml.Yaml;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class YamlParser {

    /*   static {
        try {
            loadYaml(GrootYamlParser.class.getClassLoader().getResourceAsStream("groot-platform-schedule.yaml"));
        } catch (Exception e) {
        //    propService = null;
        }
    }*/

    private static Map<String, Object> yamlData;

    public static <T> T loadYamlToEntity(Class<T> entityType) throws FileNotFoundException {
        // 创建Yaml对象
        Yaml yaml = new Yaml();

        // 从YAML文件中读取数据并转换为实体对象
        try {
            return yaml.loadAs(
                    YamlParser.class
                            .getClassLoader()
                            .getResourceAsStream("groot-platform-schedule.yaml"),
                    entityType);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Map<String, Object> loadYaml(InputStream input) throws Exception {
        // 读取YAML文件
        Map<String, Object> yamlData = new HashMap<>();
        Yaml yaml = new Yaml();
        yamlData = yaml.load(input);
        if (yamlData != null) {
            return yamlData;
        } else {
            throw new Exception("加载yaml配置文件失败");
        }
    }

    public static Object getValue(String key) {
        return getValue(key, yamlData);
    }

    public static Object getValue(String key, Map<String, Object> data) {
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

    public static Map<String, Object> getMap(String key, Map<String, Object> yamlData) {
        Object value = getValue(key, yamlData);
        if (value instanceof Map) {
            return (Map<String, Object>) value;
        }
        return null;
    }

    public static void main(String[] args) {
        /* GrootYamlParser yamlParser = new GrootYamlParser("data.yaml");

            // 获取基本信息
            String name = (String) yamlParser.getValue("name");
            int age = (int) yamlParser.getValue("age");
            String occupation = (String) yamlParser.getValue("occupation");
            System.out.println("Name: " + name);
            System.out.println("Age: " + age);
            System.out.println("Occupation: " + occupation);

            System.out.println();

            // 获取地址信息
            Map<String, Object> address = yamlParser.getMap("address");
            String street = (String) address.get("street");
            String city = (String) address.get("city");
            String country = (String) address.get("country");
            System.out.println("Street: " + street);
            System.out.println("City: " + city);
            System.out.println("Country: " + country);

            System.out.println();

            // 获取项目列表
            List<Object> projects = yamlParser.getList("projects");
            for (Object project : projects) {
                Map<String, Object> projectMap = (Map<String, Object>) project;
                String projectName = (String) projectMap.get("name");
                String projectDescription = (String) projectMap.get("description");
                List<String> teamMembers = (List<String>) projectMap.get("team");
                System.out.println("Project Name: " + projectName);
                System.out.println("Project Description: " + projectDescription);
                System.out.println("Team Members: " + teamMembers);
                System.out.println();
            }

            System.out.println();

            // 获取配置信息
            Map<String, Object> databaseConfig = yamlParser.getMap("config.database");
            String dbHost = (String) databaseConfig.get("host");
            int dbPort = (int) databaseConfig.get("port");
            String dbUsername = (String) databaseConfig.get("username");
            String dbPassword = (String) databaseConfig.get("password");
            System.out.println("Database Host: " + dbHost);
            System.out.println("Database Port: " + dbPort);
            System.out.println("Database Username: " + dbUsername);
            System.out.println("Database Password: " + dbPassword);
        }*/
    }
}
