package com.geedgenetworks.config;

import java.util.Map;

/**
 * Created by wk on 2021/1/6.
 */
public class commonConfig {


  public  static final  String NACOS_SERVER_ADDR = GrootYamlParser.getStringProperty("nacos.server_addr");
  public  static final  String NACOS_USERNAME = GrootYamlParser.getStringProperty("nacos.username");
  public  static final  String NACOS_PASSWORD= GrootYamlParser.getStringProperty("nacos.password");
  public  static final  String NACOS_NAMESPACE = GrootYamlParser.getStringProperty("nacos.namespace");
  public  static final  String NACOS_DATA_ID = GrootYamlParser.getStringProperty("nacos.data_id") ;
  public  static final  String NACOS_GROUP= GrootYamlParser.getStringProperty("nacos.group");
  public  static final long NACOS_READ_TIMEOUT= GrootYamlParser.getIntProperty("nacos.read_timeout");
  public  static final  String HOS_TOKEN= GrootYamlParser.getStringProperty("hos.token");
  public  static final   int KNOWLEDGE_FILE_CHECK_NUMBER = GrootYamlParser.getIntProperty("knowledge.file_check_number") ;

  public  static final String ID_ARRAY = GrootYamlParser.getStringProperty("knowledge.id_array");

  public static final int HTTP_POOL_MAX_CONNECTION = GrootYamlParser.getIntProperty("http.pool.max_connection");
  public static final int HTTP_POOL_MAX_PER_ROUTE = GrootYamlParser.getIntProperty("http.pool.max_per_route");
  public static final int HTTP_POOL_REQUEST_TIMEOUT = GrootYamlParser.getIntProperty("http.pool.request_timeout");
  public static final int HTTP_POOL_CONNECT_TIMEOUT = GrootYamlParser.getIntProperty("http.pool.connect_timeout");
  public static final int HTTP_POOL_RESPONSE_TIMEOUT = GrootYamlParser.getIntProperty("http.pool.response_timeout");



  public static final String GTPC_FAMILY_NAME = "gtp";
  public static final String RADIUS_FAMILY_NAME = "radius";
  public static final String COMMON_FAMILY_NAME = "common";
  public static final String DEFAULT_RELATIONSHIP_MODULE = "vsys";
  public static final String ZOOKEEPER_QUORUM = GrootYamlParser.getStringProperty("zookeeper.quorum");
  public static final long   DATA_CENTER_ID_NUM = GrootYamlParser.getIntProperty("snowid.data_center_id_num");

  public static final String KNOWLEDGEBASE_FILE_STORAGE_PATH = GrootYamlParser.getStringProperty( "knowledgebase.file_storage_path");
  public static final String KNOWLEDGEBASE_FILE_STORAGE_TYPE = GrootYamlParser.getStringProperty( "knowledgebase.file_storage_type");
  public static final String KNOWLEDGEBASE_TYPE_LIST = GrootYamlParser.getStringProperty("knowledgebase.type_list");
  public static final String KNOWLEDGEBASE_NAME_LIST = GrootYamlParser.getStringProperty( "knowledgebase.name_list");
  public static final String HDFS_SERVERS = GrootYamlParser.getStringProperty( "hdfs.servers");

/*  public static final String IPV4_USER_DEFINED_ID = GrootYamlParser.getStringProperty( "knowledge.ipv4_user_defined_id");
  public static final String IPV6_USER_DEFINED_ID = GrootYamlParser.getStringProperty( "knowledge.ipv6_user_defined_id");
  public static final String IPV4_BUILT_IN_ID = GrootYamlParser.getStringProperty( "knowledge.ipv4_built_in_id");
  public static final String IPV6_BUILT_IN_ID = GrootYamlParser.getStringProperty( "knowledge.ipv6_built_in_id");
  public static final String ASN_V4_ID = GrootYamlParser.getStringProperty( "knowledge.asn_v4_id");
  public static final String ASN_V6_ID = GrootYamlParser.getStringProperty( "knowledge.asn_v6_id");*/

  public static final Map<String,Object>  KNOWLEDGE_IPLOOKUP_MAP = GrootYamlParser.getMap( "knowledge.iplookup");
  public static final Map<String,Object>  KNOWLEDGE_ASNLOOKUP_MAP = GrootYamlParser.getMap( "knowledge.asnlookup");



  public static final Integer HBASE_TICK_TUPLE_FREQ_SECS = GrootYamlParser.getIntProperty("hbase.tick_tuple_freq_secs");
  public static final Integer HBASE_GTPC_SCAN_MAX_ROWS = GrootYamlParser.getIntProperty("hbase.gtpc_scan_max_rows");
  public static final Integer HBASE_RADIUS_SCAN_MAX_ROWS = GrootYamlParser.getIntProperty("hbase.radius_scan_max_rows");
  public static final String HBASE_RADIUS_TABLE_NAME = GrootYamlParser.getStringProperty("hbase.radius_table_name");
  public static final String HBASE_GTPC_TABLE_NAME = GrootYamlParser.getStringProperty("hbase.gtpc_table_name");
  public static final String HBASE_RPC_TIMEOUT = GrootYamlParser.getStringProperty("hbase.rpc_timeout");
  public static final String DATA_RELATIONSHIP_MODEL = GrootYamlParser.getStringProperty("data.relationship_model");


  public static void main(String[] args) {
    System.out.println("");
  }


}
