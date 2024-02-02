package com.geedgenetworks.core.utils.KnowlegdeBase.Handler.cn;

import com.geedgenetworks.core.pojo.KnowLedgeBaseFileMeta;
import com.geedgenetworks.core.utils.cn.common.Trie;
import com.geedgenetworks.core.utils.cn.csv.HighCsvReader;
import org.apache.commons.lang3.StringUtils;
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
public class DomainTagUserDefineKnowledgeBaseHandler extends AbstractMultipleKnowledgeBaseHandler {

    private static final Logger logger = LoggerFactory.getLogger(DomainTagUserDefineKnowledgeBaseHandler.class);

    private Trie<Node> fqdnTagFuzzyMatchRules = new Trie<>();
    private Map<String, List<Node>> fqdnTagFullMatchRules = new HashMap<>();

    private DomainTagUserDefineKnowledgeBaseHandler() {
    }

    private static final class InstanceHolder {
        private static final DomainTagUserDefineKnowledgeBaseHandler instance = new DomainTagUserDefineKnowledgeBaseHandler();
    }

    public static DomainTagUserDefineKnowledgeBaseHandler getInstance() {
        return InstanceHolder.instance;
    }

    @Override
    protected Boolean buildKnowledgeBase() {
        Trie<Node> newFqdnTagFuzzyMatchRules = new Trie<>();
        Map<String, List<Node>> newFqdnTagFullMatchRules = new HashMap<>();
        this.knowledgeMetedataCacheMap.forEach((key, value) -> {
            buildSingleKnowledgeBase(newFqdnTagFuzzyMatchRules, newFqdnTagFullMatchRules, convertId(key), value);
        });
        fqdnTagFuzzyMatchRules = newFqdnTagFuzzyMatchRules;
        fqdnTagFullMatchRules = newFqdnTagFullMatchRules;
        return true;
    }

    private void buildSingleKnowledgeBase(Trie<Node> fqdnTagsFuzzy, Map<String, List<Node>> fqdnTagsFull, Long id, KnowLedgeBaseFileMeta metadata) {
        try {
            List<String> needColumns = new ArrayList<>();
            needColumns.add("tag_value");
            needColumns.add("domain");
            byte[] content = downloadFile(metadata.getPath(), 1);
            HighCsvReader highCsvReader = new HighCsvReader(new InputStreamReader(new ByteArrayInputStream(content)), needColumns);
            HighCsvReader.CsvIterator iterator = highCsvReader.getIterator();
            while (iterator.hasNext()) {
                Map<String, String> line = iterator.next();
                String tagValue = line.get("tag_value");
                String rule = line.get("domain");

                if (rule == null || rule.length() == 0) {
                    continue;
                }
                if (!rule.startsWith("$") && !rule.startsWith("*")) {
                    continue;
                }
                String newRule = rule.substring(1);
                if (rule.startsWith("*")) {
                    Node node = new Node(tagValue, id);
                    fqdnTagsFuzzy.put(StringUtils.reverse(newRule), node);
                } else {
                    if (fqdnTagsFull.containsKey(newRule)) {
                        List<Node> nodes = fqdnTagsFull.get(newRule);
                        nodes.add(new Node(tagValue, id));
                    } else {
                        Node node = new Node(tagValue, id);
                        List<Node> nodes = new ArrayList<>();
                        nodes.add(node);
                        fqdnTagsFull.put(newRule, nodes);
                    }
                }
            }
        } catch (Exception e) {
            logger.error(this.getClass().getSimpleName() + " update error", e);
        }
    }

    public List<Node> lookup(String domain) {
        List<Node> nodes = new ArrayList<>();
        if (domain == null || domain.length() == 0) {
            return nodes;
        }
        List<Node> fuzzyNodes = fqdnTagFuzzyMatchRules.get(StringUtils.reverse(domain));
        nodes.addAll(fuzzyNodes);
        if (fqdnTagFullMatchRules.containsKey(domain)) {
            List<Node> fullNodes = fqdnTagFullMatchRules.get(domain);
            nodes.addAll(fullNodes);
        }
        return nodes;
    }
}
