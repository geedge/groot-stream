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
                    .defaultValue(new ArrayList<String>())
                    .withDescription("The files of knowledgebase.");


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
                    .withDescription("The general properties of grootstream");

    public static final Option<String> ZOOKEEPER_QUORUM =
            Options.key("quorum")
                    .stringType()
                    .defaultValue("")
                    .withDescription("The quorum of zookeeper.");


}
