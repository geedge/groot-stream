package com.geedgenetworks.config;

import org.yaml.snakeyaml.Yaml;

import java.util.Map;

import static com.geedgenetworks.config.GrootYamlParser.getClassReflect;
import static com.geedgenetworks.config.GrootYamlParser.loadYaml;

/**
 * Created by wk on 2021/1/6.
 */
public class commonConfig {
  public static Map<String, Object> yamlData;
  public static Map<String, Object> pluginData;

  static {
    try {
      Yaml yaml = new Yaml();
      yamlData = yaml.load(GrootYamlParser.class.getClassLoader().getResourceAsStream("groot.yaml"));
      pluginData = loadYaml(GrootYamlParser.class.getClassLoader().getResourceAsStream("groot-platform-plugin.yaml"));

      //      loadYaml(GrootYamlParser.class.getClassLoader().getResourceAsStream("groot.yaml"));
    } catch (Exception e) {
      //    propService = null;
    }
  }
  public  static final  Map<String, String> UDF_CLASS_REFLECT = getClassReflect("udf",pluginData);

  public  static final  String NACOS_SERVER_ADDR = GrootYamlParser.getStringProperty("nacos.server_addr",yamlData);
  public  static final  String NACOS_USERNAME = GrootYamlParser.getStringProperty("nacos.username",yamlData);
  public  static final  String NACOS_PASSWORD= GrootYamlParser.getStringProperty("nacos.password",yamlData);
  public  static final  String NACOS_NAMESPACE = GrootYamlParser.getStringProperty("nacos.namespace",yamlData);
  public  static final  String NACOS_DATA_ID = GrootYamlParser.getStringProperty("nacos.data_id",yamlData) ;
  public  static final  String NACOS_GROUP= GrootYamlParser.getStringProperty("nacos.group",yamlData);
  public  static final long NACOS_READ_TIMEOUT= GrootYamlParser.getIntProperty("nacos.read_timeout",yamlData);
  public  static final  String HOS_TOKEN= GrootYamlParser.getStringProperty("hos.token",yamlData);
  public  static final   int KNOWLEDGE_FILE_CHECK_NUMBER = GrootYamlParser.getIntProperty("knowledge.file_check_number",yamlData) ;

  public  static final String ID_ARRAY = GrootYamlParser.getStringProperty("knowledge.id_array",yamlData);

  public static final int HTTP_POOL_MAX_CONNECTION = GrootYamlParser.getIntProperty("http.pool.max_connection",yamlData);
  public static final int HTTP_POOL_MAX_PER_ROUTE = GrootYamlParser.getIntProperty("http.pool.max_per_route",yamlData);
  public static final int HTTP_POOL_REQUEST_TIMEOUT = GrootYamlParser.getIntProperty("http.pool.request_timeout",yamlData);
  public static final int HTTP_POOL_CONNECT_TIMEOUT = GrootYamlParser.getIntProperty("http.pool.connect_timeout",yamlData);
  public static final int HTTP_POOL_RESPONSE_TIMEOUT = GrootYamlParser.getIntProperty("http.pool.response_timeout",yamlData);



  public static final String GTPC_FAMILY_NAME = "gtp";
  public static final String RADIUS_FAMILY_NAME = "radius";
  public static final String COMMON_FAMILY_NAME = "common";
  public static final String DEFAULT_RELATIONSHIP_MODULE = "vsys";
  public static final String ZOOKEEPER_QUORUM = GrootYamlParser.getStringProperty("zookeeper.quorum",yamlData);
  public static final long   DATA_CENTER_ID_NUM = GrootYamlParser.getIntProperty("snowid.data_center_id_num",yamlData);

  public static final String KNOWLEDGEBASE_FILE_STORAGE_PATH = GrootYamlParser.getStringProperty( "knowledgebase.file_storage_path",yamlData);
  public static final String KNOWLEDGEBASE_FILE_STORAGE_TYPE = GrootYamlParser.getStringProperty( "knowledgebase.file_storage_type",yamlData);
  public static final String KNOWLEDGEBASE_TYPE_LIST = GrootYamlParser.getStringProperty("knowledgebase.type_list",yamlData);
  public static final String KNOWLEDGEBASE_NAME_LIST = GrootYamlParser.getStringProperty( "knowledgebase.name_list",yamlData);
  public static final String HDFS_SERVERS = GrootYamlParser.getStringProperty( "hdfs.servers",yamlData);

/*  public static final String IPV4_USER_DEFINED_ID = GrootYamlParser.getStringProperty( "knowledge.ipv4_user_defined_id");
  public static final String IPV6_USER_DEFINED_ID = GrootYamlParser.getStringProperty( "knowledge.ipv6_user_defined_id");
  public static final String IPV4_BUILT_IN_ID = GrootYamlParser.getStringProperty( "knowledge.ipv4_built_in_id");
  public static final String IPV6_BUILT_IN_ID = GrootYamlParser.getStringProperty( "knowledge.ipv6_built_in_id");
  public static final String ASN_V4_ID = GrootYamlParser.getStringProperty( "knowledge.asn_v4_id");
  public static final String ASN_V6_ID = GrootYamlParser.getStringProperty( "knowledge.asn_v6_id");*/

  public static final Map<String,Object>  KNOWLEDGE_IPLOOKUP_MAP = GrootYamlParser.getMap( "knowledge.iplookup",yamlData);
  public static final Map<String,Object>  KNOWLEDGE_ASNLOOKUP_MAP = GrootYamlParser.getMap( "knowledge.asnlookup",yamlData);



  public static final Integer HBASE_TICK_TUPLE_FREQ_SECS = GrootYamlParser.getIntProperty("hbase.tick_tuple_freq_secs",yamlData);
  public static final Integer HBASE_GTPC_SCAN_MAX_ROWS = GrootYamlParser.getIntProperty("hbase.gtpc_scan_max_rows",yamlData);
  public static final Integer HBASE_RADIUS_SCAN_MAX_ROWS = GrootYamlParser.getIntProperty("hbase.radius_scan_max_rows",yamlData);
  public static final String HBASE_RADIUS_TABLE_NAME = GrootYamlParser.getStringProperty("hbase.radius_table_name",yamlData);
  public static final String HBASE_GTPC_TABLE_NAME = GrootYamlParser.getStringProperty("hbase.gtpc_table_name",yamlData);
  public static final String HBASE_RPC_TIMEOUT = GrootYamlParser.getStringProperty("hbase.rpc_timeout",yamlData);
  public static final String DATA_RELATIONSHIP_MODEL = GrootYamlParser.getStringProperty("data.relationship_model",yamlData);


  public static void main(String[] args) {
    System.out.println("");
  }


}
