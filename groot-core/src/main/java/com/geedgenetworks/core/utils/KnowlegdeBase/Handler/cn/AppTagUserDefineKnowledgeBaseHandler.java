package com.geedgenetworks.core.utils.KnowlegdeBase.Handler.cn;

import com.geedgenetworks.core.utils.cn.common.KnowledgeMetadata;
import com.geedgenetworks.core.utils.cn.csv.HighCsvReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/1/22 15:05
 */
public class AppTagUserDefineKnowledgeBaseHandler extends AbstractMultipleKnowledgeBaseHandler {

    private static final Logger logger = LoggerFactory.getLogger(AppTagUserDefineKnowledgeBaseHandler.class);

    private Map<String, List<Node>> appTagRules = new HashMap<>();

    private AppTagUserDefineKnowledgeBaseHandler() {
    }

    private static final class InstanceHolder {
        private static final AppTagUserDefineKnowledgeBaseHandler instance = new AppTagUserDefineKnowledgeBaseHandler();
    }

    public static AppTagUserDefineKnowledgeBaseHandler getInstance() {
        return InstanceHolder.instance;
    }

    @Override
    protected Boolean buildKnowledgeBase() {
        Map<String, List<Node>> newAppTagRules = new HashMap<>();
        this.knowledgeMetedataCacheMap.forEach((key, value) -> {
            buildSingleKnowledgeBase(newAppTagRules, convertId(key), value);
        });
        appTagRules = newAppTagRules;
        return true;
    }

    private void buildSingleKnowledgeBase(Map<String, List<Node>> appTags, Long id, KnowledgeMetadata metadata) {
        try {
            List<String> needColumns = new ArrayList<>();
            needColumns.add("tag_value");
            needColumns.add("app_name");
            byte[] content = downloadFile(metadata.getPath(), 1);
            HighCsvReader highCsvReader = new HighCsvReader(new InputStreamReader(new ByteArrayInputStream(content)), needColumns);
            HighCsvReader.CsvIterator iterator = highCsvReader.getIterator();
            while (iterator.hasNext()) {
                Map<String, String> line = iterator.next();
                String tagValue = line.get("tag_value");
                String appName = line.get("app_name");

                if (appTags.containsKey(appName)) {
                    List<Node> nodes = appTags.get(appName);
                    nodes.add(new Node(tagValue, id));
                } else {
                    Node node = new Node(tagValue, id);
                    List<Node> nodes = new ArrayList<>();
                    nodes.add(node);
                    appTags.put(appName, nodes);
                }
            }
        } catch (Exception e) {
            logger.error(this.getClass().getSimpleName() + " update error", e);
        }
    }

    public List<Node> lookup(String app) {
        if (appTagRules.containsKey(app)) {
            return appTagRules.get(app);
        }
        return new ArrayList<>();
    }

}
