package com.geedgenetworks.common.config;

import com.alibaba.fastjson2.TypeReference;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CommonConfigOptions {

    public static final Option<List<KnowledgeBaseConfig>> KNOWLEDGE_BASE =
            Options.key("knowledge_base")
                    .type(new TypeReference<List<KnowledgeBaseConfig>>() {
                    })
                    .noDefaultValue()
                    .withDescription("The knowledge base configuration.");

    public static final Option<Map<String, String>> KNOWLEDGE_BASE_PROPERTIES =
            Options.key("properties")
                    .mapType()
                    .defaultValue(new HashMap<String, String>())
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


    public static final Option<Map<String, String>> PROPERTIES =
            Options.key("properties")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("The general properties of groot stream");

    public static final Option<Map<String, KmsConfig>> KMS =
            Options.key("kms")
                    .type(new TypeReference<Map<String, KmsConfig>>() {
                    })
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

    public static final Option<String> KMS_USERNAME = Options.key("username")
            .stringType()
            .defaultValue("")
            .withDescription("The access username of KMS.");

    public static final Option<String> KMS_PASSWORD = Options.key("password")
            .stringType()
            .defaultValue("")
            .withDescription("The access username of KMS.");

    public static final Option<String> KMS_DEFAULT_KEY_PATH = Options.key("default_key_path")
            .stringType()
            .defaultValue("")
            .withDescription("The default key path of KMS.");

    public static final Option<String> KMS_PLUGIN_KEY_PATH = Options.key("plugin_key_path")
            .stringType()
            .defaultValue("")
            .withDescription("The plugin key path of KMS.");

    public static final Option<ClientSSLConfig> SSL = Options.key("ssl")
            .type(new TypeReference<ClientSSLConfig>() {
            })
            .noDefaultValue()
            .withDescription("The ssl configuration.");

    public static final Option<Boolean> SSL_SKIP_VERIFICATION = Options.key("skip_verification")
            .booleanType()
            .defaultValue(false)
            .withDescription("The skip certificate of the configuration.");

    public static final Option<String> SSL_CA_CERTIFICATE_PATH = Options.key("ca_certificate_path")
            .stringType()
            .defaultValue("")
            .withDescription("The ca certificate file path of the configuration.");

    public static final Option<String> SSL_CERTIFICATE_PATH = Options.key("certificate_path")
            .stringType()
            .defaultValue("")
            .withDescription("The certificate file path of the configuration.");

    public static final Option<String> SSL_PRIVATE_KEY_PATH = Options.key("private_key_path")
            .stringType()
            .defaultValue("")
            .withDescription("The private key file path of the configuration.");
}
