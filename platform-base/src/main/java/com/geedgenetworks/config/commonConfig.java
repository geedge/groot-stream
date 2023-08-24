package com.geedgenetworks.config;

/**
 * Created by wk on 2021/1/6.
 */
public class commonConfig {

  public  static final  String NACOS_SERVER_ADDR = YamlParser.getStringProperty("nacos.server_addr");
  public  static final  String NACOS_USERNAME = YamlParser.getStringProperty("nacos.username");
  public  static final  String NACOS_PASSWORD= YamlParser.getStringProperty("nacos.password");
  public  static final  String NACOS_NAMESPACE = YamlParser.getStringProperty("nacos.namespace");
  public  static final  String NACOS_DATA_ID = YamlParser.getStringProperty("nacos.data_id") ;
  public  static final  String NACOS_GROUP= YamlParser.getStringProperty("nacos.group");
  public  static final long NACOS_READ_TIMEOUT= YamlParser.getIntProperty("nacos.read_timeout");
  public  static final  String HOS_TOKEN= YamlParser.getStringProperty("hos.token");
  public  static final   int KNOWLEDGE_FILE_CHECK_NUMBER = YamlParser.getIntProperty("knowledge.file_check_number") ;

  public  static final String ID_ARRAY = YamlParser.getStringProperty("knowledge.id_array");

  public static final int HTTP_POOL_MAX_CONNECTION = YamlParser.getIntProperty("http.pool.max_connection");
  public static final int HTTP_POOL_MAX_PER_ROUTE = YamlParser.getIntProperty("http.pool.max_per_route");
  public static final int HTTP_POOL_REQUEST_TIMEOUT = YamlParser.getIntProperty("http.pool.request_timeout");
  public static final int HTTP_POOL_CONNECT_TIMEOUT = YamlParser.getIntProperty("http.pool.connect_timeout");
  public static final int HTTP_POOL_RESPONSE_TIMEOUT = YamlParser.getIntProperty("http.pool.response_timeout");



  public static final String GTPC_FAMILY_NAME = "gtp";
  public static final String RADIUS_FAMILY_NAME = "radius";
  public static final String COMMON_FAMILY_NAME = "common";
  public static final String DEFAULT_RELATIONSHIP_MODULE = "vsys";
  public static final String SNOWID_ZK = YamlParser.getStringProperty("snowid.zookeeper");
  public static final long  DATA_CENTER_ID_NUM = YamlParser.getIntProperty("snowid.data_center_id_num");

  public static final String KNOWLEDGEBASE_FILE_STORAGE_PATH = YamlParser.getStringProperty( "knowledgebase.file_storage_path");
  public static final String KNOWLEDGEBASE_FILE_STORAGE_TYPE = YamlParser.getStringProperty( "knowledgebase.file_storage_type");
  public static final String KNOWLEDGEBASE_TYPE_LIST = YamlParser.getStringProperty("knowledgebase.type_list");
  public static final String KNOWLEDGEBASE_NAME_LIST = YamlParser.getStringProperty( "knowledgebase.name_list");
  public static final String HDFS_SERVERS = YamlParser.getStringProperty( "hdfs.servers");

  public static final String IPV4_USER_DEFINED_ID = YamlParser.getStringProperty( "knowledge.ipv4_user_defined_id");
  public static final String IPV6_USER_DEFINED_ID = YamlParser.getStringProperty( "knowledge.ipv6_user_defined_id");
  public static final String IPV4_BUILT_IN_ID = YamlParser.getStringProperty( "knowledge.ipv4_built_in_id");
  public static final String IPV6_BUILT_IN_ID = YamlParser.getStringProperty( "knowledge.ipv6_built_in_id");
  public static final String ASN_V4_ID = YamlParser.getStringProperty( "knowledge.asn_v4_id");
  public static final String ASN_V6_ID = YamlParser.getStringProperty( "knowledge.asn_v6_id");



  public static final Integer HBASE_TICK_TUPLE_FREQ_SECS = YamlParser.getIntProperty("hbase.tick_tuple_freq_secs");
  public static final Integer HBASE_GTPC_SCAN_MAX_ROWS = YamlParser.getIntProperty("hbase.gtpc_scan_max_rows");
  public static final Integer HBASE_RADIUS_SCAN_MAX_ROWS = YamlParser.getIntProperty("hbase.radius_scan_max_rows");
  public static final String HBASE_RADIUS_TABLE_NAME = YamlParser.getStringProperty("hbase.radius_table_name");
  public static final String HBASE_GTPC_TABLE_NAME = YamlParser.getStringProperty("hbase.gtpc_table_name");
  public static final String HBASE_RPC_TIMEOUT = YamlParser.getStringProperty("hbase.rpc_timeout");
  public static final String DATA_RELATIONSHIP_MODEL = YamlParser.getStringProperty("data.relationship_model");


  public static void main(String[] args) {
    System.out.println("");
  }


}
