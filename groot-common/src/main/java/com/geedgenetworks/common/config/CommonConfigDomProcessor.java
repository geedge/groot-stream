package com.geedgenetworks.common.config;

import com.hazelcast.internal.config.AbstractDomConfigProcessor;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import lombok.extern.slf4j.Slf4j;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.internal.config.DomConfigHelper.*;

@Slf4j
public class CommonConfigDomProcessor extends AbstractDomConfigProcessor {
    private final GrootStreamConfig config;
    CommonConfigDomProcessor(boolean domLevel3, GrootStreamConfig config) {
        super(domLevel3);
        this.config = config;
    }

    @Override
    public void buildConfig(Node rootNode) {
        final CommonConfig commonConfig = config.getCommonConfig();
        for (Node node : childElements(rootNode)) {
            String name = cleanNodeName(node);
             if (CommonConfigOptions.KNOWLEDGE_BASE.key().equals(name)) {
                 commonConfig.setKnowledgeBaseConfig(parseKnowledgeBaseConfig(node));
            } else if (CommonConfigOptions.PROPERTIES.key().equals(name)) {
                 commonConfig.setPropertiesConfig(parsePropertiesConfig(node));
            } else {
                log.warn("Unrecognized configuration element: " + name);
            }

        }
    }

    private Map<String, String> parsePropertiesConfig(Node properties) {

            Map<String, String> propertiesMap = new HashMap<>();
            for (Node node : childElements(properties)) {
                String name = cleanNodeName(node);
                propertiesMap.put(name,getTextContent(node));
            }
            return propertiesMap;
    }


    private List<KnowledgeBaseConfig> parseKnowledgeBaseConfig(Node kbRootNode) {

        List<KnowledgeBaseConfig> knowledgeConfigList = new ArrayList<>();
        for (Node node : childElements(kbRootNode)) {
            knowledgeConfigList.add(parseKnowledgeBaseConfigAsObject(node));
        }
        return knowledgeConfigList;
    }

   private KnowledgeBaseConfig parseKnowledgeBaseConfigAsObject(Node kbNode) {
        KnowledgeBaseConfig knowledgeBaseConfig = new KnowledgeBaseConfig();
        for (Node node : childElements(kbNode)) {
            String name = cleanNodeName(node);

            if (CommonConfigOptions.KNOWLEDGE_BASE_NAME.key().equals(name)) {
                knowledgeBaseConfig.setName(getTextContent(node));
            } else if (CommonConfigOptions.KNOWLEDGE_BASE_FS_TYPE.key().equals(name)) {
                knowledgeBaseConfig.setFsType(getTextContent(node));
            } else if (CommonConfigOptions.KNOWLEDGE_BASE_FS_PATH.key().equals(name)) {
                knowledgeBaseConfig.setFsPath(getTextContent(node));
            } else if (CommonConfigOptions.KNOWLEDGE_BASE_FILES.key().equals(name)) {
                knowledgeBaseConfig.setFiles(parseKnowledgeBaseFilesConfig(node));
            } else if (CommonConfigOptions.KNOWLEDGE_BASE_PROPERTIES.key().equals(name)) {
                knowledgeBaseConfig.setProperties(parseKnowledgeBasePropertiesConfig(node));
            }
            else{
                log.warn("Unrecognized configuration element: " + name);
            }
        }
        return knowledgeBaseConfig;
    }

    private Map<String, String> parseKnowledgeBasePropertiesConfig(Node properties) {
        Map<String, String> propertiesMap = new HashMap<>();
        for (Node node : childElements(properties)) {
            String name = cleanNodeName(node);
            propertiesMap.put(name,getTextContent(node));
        }
        return propertiesMap;
    }

    private List<String> parseKnowledgeBaseFilesConfig(Node files) {

        List<String> asnLookupConfigList = new ArrayList<>();
        for (Node node : childElements(files)) {
            String value = node.getNodeValue();
            asnLookupConfigList.add(value);
        }
        return asnLookupConfigList;
    }


}
