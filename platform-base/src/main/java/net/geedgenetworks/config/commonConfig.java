package net.geedgenetworks.config;

/**
 * Created by wk on 2021/1/6.
 */
public class commonConfig {

  public static final String GTPC_FAMILY_NAME = "gtp";
  public static final String RADIUS_FAMILY_NAME = "radius";
  public static final String COMMON_FAMILY_NAME = "common";
  public static final String DEFAULT_RELATIONSHIP_MODULE = "vsys";
  public static final String SNOWID_ZK = commonConfigurations.getStringProperty("snowid.zookeeper");
  public static final long  DATA_CENTER_ID_NUM =commonConfigurations.getLongProperty("data.center.id.num");

  public static final String KNOWLEDGEBASE_FILE_STORAGE_PATH = commonConfigurations.getStringProperty( "knowledgebase.file.storage.path");
  public static final String KNOWLEDGEBASE_FILE_STORAGE_TYPE = commonConfigurations.getStringProperty( "knowledgebase.file.storage.type");
  public static final String KNOWLEDGEBASE_TYPE_LIST = commonConfigurations.getStringProperty("knowledgebase.type.list");
  public static final String KNOWLEDGEBASE_NAME_LIST = commonConfigurations.getStringProperty( "knowledgebase.name.list");
  public static final String HDFS_SERVERS = commonConfigurations.getStringProperty( "hdfs.servers");


  public static final Integer HBASE_TICK_TUPLE_FREQ_SECS = commonConfigurations.getIntProperty("hbase.tick.tuple.freq.secs");
  public static final Integer HBASE_GTPC_SCAN_MAX_ROWS = commonConfigurations.getIntProperty("hbase.gtpc.scan.max.rows");
  public static final Integer HBASE_RADIUS_SCAN_MAX_ROWS = commonConfigurations.getIntProperty("hbase.radius.scan.max.rows");
  public static final String HBASE_RADIUS_TABLE_NAME = commonConfigurations.getStringProperty("hbase.radius.table.name");
  public static final String HBASE_GTPC_TABLE_NAME = commonConfigurations.getStringProperty("hbase.gtpc.table.name");
  public static final String HBASE_RPC_TIMEOUT = commonConfigurations.getStringProperty("hbase.rpc.timeout");
  public static final String DATA_RELATIONSHIP_MODEL = commonConfigurations.getStringProperty("data.relationship.model");

}
