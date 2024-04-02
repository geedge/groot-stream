package com.geedgenetworks.core.udf.knowlegdebase.handler;

import com.alibaba.fastjson2.JSON;
import com.geedgenetworks.common.config.KnowledgeBaseConfig;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import lombok.Data;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/1/22 16:35
 */
public class RuleKnowledgeBaseHandler extends AbstractKnowledgeBaseHandler {

    private static final Logger logger = LoggerFactory.getLogger(RuleKnowledgeBaseHandler.class);

    protected KnowledgeBaseConfig knowledgeBaseConfig;

    private static final CloseableHttpClient HTTP_CLIENT = HttpClients.createMinimal();

    private Long currentVersion;

    private Map<Long, Rule> ruleMap = new HashMap<>();

    private Map<String, Rule> nameMap = new HashMap<>();

    private Map<Long, List<Rule>> kbIdMap = new HashMap<>();

    private RuleKnowledgeBaseHandler() {
    }

    private static final class InstanceHolder {
        private static final RuleKnowledgeBaseHandler instance = new RuleKnowledgeBaseHandler();
    }

    public static RuleKnowledgeBaseHandler getInstance() {
        return InstanceHolder.instance;
    }

    @Override
    public boolean initKnowledgeBase(KnowledgeBaseConfig knowledgeBaseConfig) {
        this.knowledgeBaseConfig = knowledgeBaseConfig;
        updateCache();
        return true;
    }

    @Override
    public void updateKnowledgeBase() {
        updateCache();
    }

    public void updateCache() {
        if ("http".equals(knowledgeBaseConfig.getFsType())) {
            requestApi();
            return;
        }
        if ("local".equals(knowledgeBaseConfig.getFsType())) {
            byte[] localRuleContent = getFileFromLocal(knowledgeBaseConfig.getFsPath());
            RuleResponse ruleResponse = JSON.parseObject(new String(localRuleContent), RuleResponse.class);
            processResponse(ruleResponse);
            return;
        }
        throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, knowledgeBaseConfig.getFsType() + " is illegal");
    }

    private void requestApi() {
        String url = getUrl(knowledgeBaseConfig.getFsPath());
        final HttpGet httpGet = new HttpGet(url);
        httpGet.addHeader("Accept", "application/json");
        httpGet.addHeader("Cn-Authorization", knowledgeBaseConfig.getProperties().get("token"));
        try {
            CloseableHttpResponse response = HTTP_CLIENT.execute(httpGet);
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                String content = EntityUtils.toString(entity, "UTF-8");
                RuleResponse ruleResponse = JSON.parseObject(content, RuleResponse.class);
                processResponse(ruleResponse);
            }
        } catch (IOException e) {
            logger.error("fetch rule metadata error", e);
        }
    }

    private void processResponse(RuleResponse ruleResponse) {
        if (ruleResponse.data.version.equals(this.currentVersion)) {
            return;
        }
        this.currentVersion = ruleResponse.data.version;
        List<Rule> addList = ruleResponse.data.addList;
        addList.addAll(ruleResponse.data.updateList);
        List<Long> deleteIds = ruleResponse.data.deleteIds;
        addList.forEach(rule -> rule.setName(rule.getName().toLowerCase()));
        addList.stream().filter(rule -> rule.isBuiltIn == 1).forEach(rule -> {
            nameMap.put(rule.name, rule);
            ruleMap.put(rule.ruleId, rule);
        });
        addList.stream().filter(rule -> rule.isBuiltIn != 1).forEach(rule -> {
            kbIdMap.computeIfAbsent(rule.ruleConfigObj.getKnowledgeBase().getKnowledgeId(), k -> new ArrayList<Rule>()).add(rule);
            ruleMap.put(rule.ruleId, rule);
        });
        deleteIds.forEach(id -> {
            Rule remove = ruleMap.remove(id);
            if (remove.isBuiltIn == 1) {
                nameMap.remove(remove.name);
            } else {
                Long knowledgeId = remove.ruleConfigObj.getKnowledgeBase().getKnowledgeId();
                if (kbIdMap.containsKey(knowledgeId)) {
                    List<Rule> rules = kbIdMap.get(knowledgeId);
                    rules.removeIf(next -> next.ruleId == remove.ruleId);
                }
            }
        });
    }

    private String getUrl(String address) {
        if (currentVersion == null) {
            return address + "/v1/rule/detection";
        }
        return address + "/v1/rule/detection/increase/" + currentVersion;
    }

    public Rule lookupByName(String name) {
        if (name == null) {
            return null;
        }
        return nameMap.get(name.toLowerCase());
    }

    public List<Rule> lookupByKbId(Long kbId) {
        return kbIdMap.get(kbId);
    }

    public void close() {
        ruleMap.clear();
        nameMap.clear();
        kbIdMap.clear();
    }

    @Data
    private static final class RuleResponse {
        private Integer code;
        private String msg;
        private DataValue data;
    }

    @Data
    private static final class DataValue {
        private Long version;
        private List<Rule> addList;
        private List<Rule> updateList;
        private List<Long> deleteIds;
    }

    @Data
    public static final class Rule {
        private long ruleId;
        private String ruleType;
        private String name;
        private String category;
        private String eventType;
        // Whether the rule is built-in (true) or custom (false).
        private int isBuiltIn;
        // Whether the current rule is enabled (true) or disabled (false).
        private int status;
        private RuleConfig ruleConfigObj;
        private Trigger ruleTriggerObj;
    }

    @Data
    private static final class RuleConfig {
        private String knowledgeId;
        private KnowledgeBase knowledgeBase;
        private String level;
        private String dataSource;
    }

    @Data
    private static final class KnowledgeBase {
        private Long knowledgeId;
        private String source;
        private String name;
        private String category;
    }

    @Data
    private static final class Trigger {
        private int atLeast;
        private String interval;
        private String resetInterval;
    }
}
