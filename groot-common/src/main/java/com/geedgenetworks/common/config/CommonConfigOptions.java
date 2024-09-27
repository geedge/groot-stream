package com.geedgenetworks.common.config;

import com.alibaba.fastjson2.TypeReference;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CommonConfigOptions {

    public static final Option<Map<String, String>> KNOWLEDGE_BASE_PROPERTIES =
            Options.key("properties")
                    .mapType()
                    .defaultValue(new HashMap<String,String>())
                    .withDescription("The properties of knowledge base");
    public static final Option<String> KNOWLEDGE_BASE_NAME =
            Options.key("name")
                    .stringType()
                    .defaultValue("")
                    .withDescription("The name of knowledge base.");
    public static final Option<String> KNOWLEDGE_BASE_FS_TYPE =
            Options.key("fs_type")
                    .stringType()
                    .defaultValue("")
                    .withDescription("The filesystem type of knowledge base.");
    public static final Option<String> KNOWLEDGE_BASE_FS_PATH =
            Options.key("fs_path")
                    .stringType()
                    .defaultValue("")
                    .withDescription("The filesystem path of knowledge base.");
    public static final Option<List<String>> KNOWLEDGE_BASE_FILES =
            Options.key("files")
                    .listType()
                    .defaultValue(new ArrayList<String>())
                    .withDescription("The files of knowledge base.");

    public static final Option<String> KNOWLEDGE_BASE_STORAGE_FS_TYPE = Options.key("fs_type")
            .stringType()
            .defaultValue("localfile")
            .withDescription("The fs type of knowledge base storage.");

    public static final Option<String> KNOWLEDGE_BASE_STORAGE_FS_DEFAULT_PATH = Options.key("fs_default_path")
            .stringType()
            .defaultValue("")
            .withDescription("The default path of knowledge base storage.");

    public static final Option<List<KnowledgeBaseConfig>> KNOWLEDGE_BASE =
            Options.key("knowledge_base")
                    .type(new TypeReference<List<KnowledgeBaseConfig>>() {})
                    .noDefaultValue()
                    .withDescription("The knowledge base configuration.");

    public static final Option<Map<String, String>> PROPERTIES =
            Options.key("properties")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("The general properties of groot stream");

    public static final Option<Map<String, KmsConfig>> KMS =
            Options.key("kms")
                    .type(new TypeReference<Map<String, KmsConfig>>() {})
                    .noDefaultValue()
                    .withDescription("The kms configuration.");

    public static final Option<String> KMS_TYPE = Options.key("type")
            .stringType()
            .defaultValue("local")
            .withDescription("The type of KMS.");

    public static final Option<String> KMS_URL = Options.key("url")
            .stringType()
            .defaultValue("")
            .withDescription("The access url of KMS.");

    public static final Option<String> KMS_TOKEN = Options.key("token")
            .stringType()
            .defaultValue("")
            .withDescription("The access token of KMS.");

    public static final Option<String> KMS_KEY_PATH = Options.key("key_path")
            .stringType()
            .defaultValue("")
            .withDescription("The key path of KMS.");

    public static final Option<SSLConfig> SSL = Options.key("ssl")
            .type(new TypeReference<SSLConfig>() {})
            .noDefaultValue()
            .withDescription("The ssl configuration.");

    public static final Option<Boolean> SSL_ENABLED = Options.key("enabled")
            .booleanType()
            .defaultValue(false)
            .withDescription("The enabled flag of the configuration.");

    public static final Option<String> SSL_CERT_FILE = Options.key("cert_file")
            .stringType()
            .defaultValue("")
            .withDescription("The certificate file path of the configuration.");

    public static final Option<String> SSL_KEY_FILE = Options.key("key_file")
            .stringType()
            .defaultValue("")
            .withDescription("The private key file path of the configuration.");

    public static final Option<Boolean> SSL_REQUIRE_CLIENT_AUTH = Options.key("require_client_auth")
            .booleanType()
            .defaultValue(false)
            .withDescription("The require client auth flag of the configuration.");




}
