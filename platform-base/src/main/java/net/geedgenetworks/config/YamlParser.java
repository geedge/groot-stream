package net.geedgenetworks.config;

import org.yaml.snakeyaml.Yaml;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;


public final class YamlParser {


    static {
        try {

            loadYaml(YamlParser.class.getClassLoader().getResourceAsStream("global.yaml"));
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
        if (yamlData instanceof Map) {
            return ((Map) yamlData).get(propertyName);
        } else if (yamlData instanceof List) {
            for (Object item : (List) yamlData) {
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


    public static String getStringProperty(String key) {

        return getYamlValue(yamlData,key,String.class);


    }

    public static Integer getIntProperty( String key) {

        return getYamlValue(yamlData, key, Integer.class);

    }

    public static Long getLongProperty(String key) {
        return getYamlValue(yamlData, key, Long.class);

    }

    public static Boolean getBooleanProperty(String key) {
        return "true".equals(getValue(key).toString().toLowerCase().trim());
    }
    public static Object getNestedValue(Map<String, Object> data, String key) {
        String[] keys = key.split("\\.");

        if (keys.length == 0) {
            return null;
        }

        Object value = data;
        for (String k : keys) {
            if (value instanceof Map) {
                if (((Map<?, ?>) value).containsKey(k)) {
                    value = ((Map<?, ?>) value).get(k);
                } else {
                    value = null;
                    break;
                }
            } else {
                value = null;
                break;
            }
        }

        return value;
    }

/*    public static Object getNestedValue(Map<String, Object> yamlData, String key) {
        String[] keys = key.split("\\.");

        if (keys.length == 0) {
            return null;
        }

        Object value = yamlData.get(keys[0]);

        if (keys.length == 1) {
            return value;
        }

        if (value instanceof Map) {
            // 使用递归继续获取下一级的value
            String nestedKey = key.substring(keys[0].length() + 1);
            return getNestedValue((Map<String, Object>) value, nestedKey);
        }

        return null;
    }*/

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


    public static Object getValue(String key) {
        return getNestedValue(yamlData,key);
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
        System.out.println();
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