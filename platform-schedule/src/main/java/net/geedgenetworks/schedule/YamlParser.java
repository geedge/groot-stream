package net.geedgenetworks.schedule;

import org.yaml.snakeyaml.Yaml;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;


public final class YamlParser {


    static {
        try {
            loadYaml(YamlParser.class.getClassLoader().getResourceAsStream("business.yaml"));
        } catch (Exception e) {
        //    propService = null;
        }
    }

    private static Map<String, Object> yamlData;

    public static <T> T loadYamlToEntity(Class<T> entityType) throws FileNotFoundException {
        // 创建Yaml对象
        Yaml yaml = new Yaml();

        // 从YAML文件中读取数据并转换为实体对象
        try  {
            return yaml.loadAs(YamlParser.class.getClassLoader().getResourceAsStream("business.yaml"), entityType);
        }  catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    public static <T> T loadYamlPropertyToEntity( String propertyName, Class<T> entityType) throws FileNotFoundException {
        // 创建Yaml对象
        Yaml yaml = new Yaml();

        // 从YAML文件中读取数据
        try {
            Object yamlData = yaml.load(YamlParser.class.getClassLoader().getResourceAsStream("business.yaml"));

            // 获取指定属性的值并转换为实体对象
            Object propertyValue = getProperty(yamlData, propertyName);
            if (propertyValue != null) {
                return yaml.loadAs(yaml.dump(propertyValue), entityType);
            } else {
                throw new IllegalArgumentException("Property not found: " + propertyName);
            }
        } catch (Exception e) {
            throw new FileNotFoundException("YAML file not found: " );
        }
    }

    // 递归地获取YAML属性的值
    private static Object getProperty(Object yamlData, String propertyName) {
        if (yamlData instanceof java.util.Map) {
            return ((java.util.Map) yamlData).get(propertyName);
        } else if (yamlData instanceof java.util.List) {
            for (Object item : (java.util.List) yamlData) {
                Object propertyValue = getProperty(item, propertyName);
                if (propertyValue != null) {
                    return propertyValue;
                }
            }
        }
        return null;
    }


    /*public YamlParser(String filePath) {
        loadYaml(filePath);
    }*/

    private static void loadYaml(InputStream input) {
        // 读取YAML文件
        //FileInputStream input = new FileInputStream(filePath);

        // 创建YAML对象
        Yaml yaml = new Yaml();

        // 解析YAML文件并获取根对象
        yamlData = yaml.load(input);

    }

    public static Object getValue(String key) {
        return getValue(key, yamlData);
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

    public static List<Object> getList(String key) {
        Object value = getValue(key);
        if (value instanceof List) {
            return (List<Object>) value;
        }
        return null;
    }

    public static Map<String, Object> getMap(String key) {
        Object value = getValue(key);
        if (value instanceof Map) {
            return (Map<String, Object>) value;
        }
        return null;
    }

    public static void main(String[] args) {
       /* YamlParser yamlParser = new YamlParser("data.yaml");

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
}}