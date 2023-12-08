package com.geedgenetworks.common.config;

import com.alibaba.fastjson2.TypeReference;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ServerConfigOptions {


    public static final Option<Integer> HTTP_CON_POOL_MAX_TOTAL =
            Options.key("max_total")
                    .intType()
                    .defaultValue(100)
                    .withDescription("The max connections of http con pool.");

    public static final Option<Integer> HTTP_CON_POOL_MAX_PER_ROUTE =
            Options.key("max_per_route")
                    .intType()
                    .defaultValue(10)
                    .withDescription("The max per route of http con pool.");

    public static final Option<Integer> HTTP_CON_POOL_CONNECTION_REQUEST_TIMEOUT =
            Options.key("connection_request_timeout")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("The connection request timeout of http con pool.");
    public static final Option<Integer> HTTP_CON_POOL_CONNECT_TIMEOUT =
            Options.key("connect_timeout")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("The connect timeout of http con pool.");

    public static final Option<Integer> HTTP_CON_POOL_SOCKET_TIMEOUT =
            Options.key("socket_timeout")
                    .intType()
                    .defaultValue(60000)
                    .withDescription("The socket timeout of http con pool.");

    public static final Option<HttpConPoolConfig> HTTP_CON_POOL = Options.key("http_con_pool")
            .type(new TypeReference<HttpConPoolConfig>() {})
            .defaultValue(new HttpConPoolConfig())
            .withDescription("The http con pool configuration.");

    public static final Option<Map<String, String>> KNOWLEDGE_BASE_PROPERTIES =
            Options.key("properties")
                    .mapType()
                    .defaultValue(new HashMap<String,String>())
                    .withDescription("The properties of knowledgebase");
    public static final Option<String> KNOWLEDGE_BASE_NAME =
            Options.key("name")
                    .stringType()
                    .defaultValue("")
                    .withDescription("The name of knowledgebase.");
    public static final Option<String> KNOWLEDGE_BASE_TYPE =
            Options.key("type")
                    .stringType()
                    .defaultValue("")
                    .withDescription("The type of knowledgebase.");

    public static final Option<List<String>> KNOWLEDGE_BASE_FILES =
            Options.key("files")
                    .listType()
                    .defaultValue(new ArrayList<>())
                    .withDescription("The files of knowledgebase.");


    public static final Option<String> KNOWLEDGE_BASE_STORAGE_FS_TYPE = Options.key("fs_type")
            .stringType()
            .defaultValue("localfile")
            .withDescription("The fs type of knowledge base storage.");

    public static final Option<String> KNOWLEDGE_BASE_STORAGE_FS_DEFAULT_PATH = Options.key("fs_default_path")
            .stringType()
            .defaultValue("")
            .withDescription("The default path of knowledge base storage.");



    public static final Option<List<KnowledgeConfig>> KNOWLEDGE_BASE =
            Options.key("knowledge_base")
                    .type(new TypeReference<List<KnowledgeConfig>>() {})
                    .defaultValue(new ArrayList<>())
                    .withDescription("The knowledge base configuration.");

    public static final Option<String> SNOWFLAKE_DATA_CENTER_ID =
            Options.key("data_center_id_num")
                    .stringType()
                    .defaultValue("")
                    .withDescription("The data center id of snowflake.");

    public static  final Option<SnowflakeConfig> SNOWFLAKE =
            Options.key("snowflake")
                    .type(new TypeReference<SnowflakeConfig>() {})
                    .defaultValue(new SnowflakeConfig())
                    .withDescription("The snowflake configuration.");


    public static final Option<String> NACOS_SERVER_ADDR =
                Options.key("server_addr")
                        .stringType()
                        .defaultValue("")
                        .withDescription("The server address of nacos.");
    public static final Option<String> NACOS_USERNAME =
                Options.key("username")
                        .stringType()
                        .defaultValue("nacos")
                        .withDescription("The username of nacos.");
    public static final Option<String> NACOS_PASSWORD =
                Options.key("password")
                        .stringType()
                        .defaultValue("nacos")
                        .withDescription("The password of nacos.");
    public static final Option<String> NACOS_NAMESPACE =
                Options.key("namespace")
                        .stringType()
                        .defaultValue("public")
                        .withDescription("The namespace of nacos.");
    public static final Option<String> NACOS_DATA_ID =
                Options.key("data_id")
                        .stringType()
                        .defaultValue("")
                        .withDescription("The data id of nacos.");
    public static final Option<String> NACOS_GROUP =
                Options.key("group")
                        .stringType()
                        .defaultValue("DEFAULT_GROUP")
                        .withDescription("The group of nacos.");
    public static final Option<Integer> NACOS_READ_TIMEOUT =
                Options.key("read_timeout")
                        .intType()
                        .defaultValue(5000)
                        .withDescription("The read timeout of nacos.");

    public static final Option<NacosConfig> NACOS =
                Options.key("nacos")
                        .type(new TypeReference<NacosConfig>() {})
                        .defaultValue(new NacosConfig())
                        .withDescription("The nacos configuration.");

    public static final Option<String> CONSUL_SERVER_ADDR = Options.key("server_addr")
                        .stringType()
                        .defaultValue("")
                        .withDescription("The server address of consul.");
    public static final Option<Integer> CONSUL_SERVER_PORT =
                Options.key("server_port")
                        .intType()
                        .defaultValue(8500)
                        .withDescription("The server port of consul.");
    public static final Option<String> CONSUL_TOKEN =
                Options.key("token")
                        .stringType()
                        .defaultValue("")
                        .withDescription("The token of consul.");

    public static final Option<ConsulConfig> CONSUL =
                Options.key("consul")
                        .type(new TypeReference<ConsulConfig>() {})
                        .defaultValue(new ConsulConfig())
                        .withDescription("The consul configuration.");

    public static final Option<String> HOS_TOKEN =
                Options.key("token")
                        .stringType()
                        .defaultValue("")
                        .withDescription("The token of hos.");

    public static final Option<HosConfig> HOS =
                Options.key("hos")
                        .type(new TypeReference<HosConfig>() {})
                        .defaultValue(new HosConfig())
                        .withDescription("The hos configuration.");


    public static final Option<String> ZOOKEEPER_QUORUM =
                Options.key("quorum")
                        .stringType()
                        .defaultValue("")
                        .withDescription("The quorum of zookeeper.");

    public static final Option<ZookeeperConfig> ZOOKEEPER =
                Options.key("zookeeper")
                        .type(new TypeReference<ZookeeperConfig>() {})
                        .defaultValue(new ZookeeperConfig())
                        .withDescription("The zookeeper configuration.");

    public static final Option<String> HDFS_SERVERS =
                Options.key("servers")
                        .stringType()
                        .defaultValue("")
                        .withDescription("The servers of hdfs.");

    public static final Option<HdfsConfig> HDFS = Options.key("hdfs")
            .type(new TypeReference<HdfsConfig>() {})
            .defaultValue(new HdfsConfig())
            .withDescription("The hdfs configuration.");



}
