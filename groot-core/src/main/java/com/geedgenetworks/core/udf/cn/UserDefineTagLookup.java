package com.geedgenetworks.core.udf.cn;

import com.geedgenetworks.core.udf.knowlegdebase.KnowledgeBaseUpdateJob;
import com.geedgenetworks.core.udf.knowlegdebase.handler.*;
import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.connector.event.Event;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;

import java.util.ArrayList;
import java.util.List;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/1/22 15:02
 */
public class UserDefineTagLookup extends AbstractKnowledgeWithRuleScalarFunction {

    private String option;

    private IpTagUserDefineKnowledgeBaseHandler ipKnowledgeBaseHandler;
    private DomainTagUserDefineKnowledgeBaseHandler domainKnowledgeBaseHandler;
    private AppTagUserDefineKnowledgeBaseHandler appKnowledgeBaseHandler;
    private RuleKnowledgeBaseHandler ruleKnowledgeBaseHandler;

    private Counter lookupTagsCounter;

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {
        option = udfContext.getParameters().get("option").toString();
        super.open(runtimeContext, udfContext);
        lookupTagsCounter = metrics.counter("lookup_tags");
    }

    @Override
    @SuppressWarnings("unchecked")
    public Event evaluate(Event event) {
        if (event.getExtractedFields().get(lookupFieldName) != null && event.getExtractedFields().get(lookupFieldName).toString().length() != 0) {
            lookUpExistCounter.inc();
            String lookupValue = event.getExtractedFields().get(lookupFieldName).toString();
            List<String> tags = new ArrayList<>();
            RuleMetadata ruleMetadata = new RuleMetadata();
            switch (option) {
                case "IP_TO_TAG":
                    List<AbstractMultipleKnowledgeBaseHandler.Node> ipNodes = ipKnowledgeBaseHandler.lookup(lookupValue);
                    if (ipNodes != null && ipNodes.size() > 0) {
                        hitCounter.inc();
                        ipNodes.forEach(node -> {
                            lookupTagsCounter.inc();
                            tags.add(node.getTag());
                            List<RuleKnowledgeBaseHandler.Rule> rules = ruleKnowledgeBaseHandler.lookupByKbId(node.getKbId());
                            if (rules != null) {
                                ruleHitCounter.inc();
                                rules.forEach(rule -> ruleMetadata.addRule(rule.getRuleId(), IocType.IP.getType()));
                            }
                        });
                    }
                    break;
                case "DOMAIN_TO_TAG":
                    List<AbstractMultipleKnowledgeBaseHandler.Node> domainNodes = domainKnowledgeBaseHandler.lookup(lookupValue);
                    if (domainNodes != null && domainNodes.size() > 0) {
                        hitCounter.inc();
                        domainNodes.forEach(node -> {
                            lookupTagsCounter.inc();
                            tags.add(node.getTag());
                            List<RuleKnowledgeBaseHandler.Rule> rules = ruleKnowledgeBaseHandler.lookupByKbId(node.getKbId());
                            if (rules != null) {
                                ruleHitCounter.inc();
                                rules.forEach(rule -> ruleMetadata.addRule(rule.getRuleId(), IocType.DOMAIN.getType()));
                            }
                        });
                    }
                    break;
                case "APP_TO_TAG":
                    List<AbstractMultipleKnowledgeBaseHandler.Node> appNodes = appKnowledgeBaseHandler.lookup(lookupValue);
                    if (appNodes != null && appNodes.size() > 0) {
                        hitCounter.inc();
                        appNodes.forEach(node -> {
                            lookupTagsCounter.inc();
                            tags.add(node.getTag());
                            List<RuleKnowledgeBaseHandler.Rule> rules = ruleKnowledgeBaseHandler.lookupByKbId(node.getKbId());
                            if (rules != null) {
                                ruleHitCounter.inc();
                                rules.forEach(rule -> ruleMetadata.addRule(rule.getRuleId(), IocType.APP.getType()));
                            }
                        });
                    }
                    break;
                default:
                    break;
            }
            event.getExtractedFields().put(outputFieldName, tags);
            (((ArrayList<Long>) (event.getExtractedFields().computeIfAbsent(internalRuleIdListFieldName, k -> new ArrayList<Long>())))).addAll(ruleMetadata.getRuleIds());
            (((ArrayList<String>) (event.getExtractedFields().computeIfAbsent(internalIocTypeListFieldName, k -> new ArrayList<String>())))).addAll(ruleMetadata.getIocTypes());
        }
        return event;
    }

    @Override
    public String functionName() {
        return "CN_USER_DEFINE_TAG_LOOKUP";
    }

    @Override
    public void close() {

    }

    @Override
    protected void registerKnowledges() {
        ruleKnowledgeBaseHandler = RuleKnowledgeBaseHandler.getInstance();
        KnowledgeBaseUpdateJob.registerKnowledgeBase(ruleKnowledgeBaseHandler, ruleConfigs.get(0));

        switch (option) {
            case "IP_TO_TAG":
                ipKnowledgeBaseHandler = IpTagUserDefineKnowledgeBaseHandler.getInstance();
                KnowledgeBaseUpdateJob.registerKnowledgeBase(ipKnowledgeBaseHandler, knowledgeBaseConfigs.get(0));
                break;
            case "DOMAIN_TO_TAG":
                domainKnowledgeBaseHandler = DomainTagUserDefineKnowledgeBaseHandler.getInstance();
                KnowledgeBaseUpdateJob.registerKnowledgeBase(domainKnowledgeBaseHandler, knowledgeBaseConfigs.get(0));
                break;
            case "APP_TO_TAG":
                appKnowledgeBaseHandler = AppTagUserDefineKnowledgeBaseHandler.getInstance();
                KnowledgeBaseUpdateJob.registerKnowledgeBase(appKnowledgeBaseHandler, knowledgeBaseConfigs.get(0));
                break;
            default:
                break;
        }
    }
}
