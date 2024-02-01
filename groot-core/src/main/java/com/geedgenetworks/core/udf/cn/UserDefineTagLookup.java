package com.geedgenetworks.core.udf.cn;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.udf.UDFContext;
import com.geedgenetworks.core.utils.KnowlegdeBase.Handler.cn.*;
import com.geedgenetworks.core.utils.KnowlegdeBase.KnowledgeBaseUpdateJob;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.util.ArrayList;
import java.util.List;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/1/22 15:02
 */
public class UserDefineTagLookup extends AbstractKnowledgeWithRuleUDF {

    private String option;

    private IpTagUserDefineKnowledgeBaseHandler ipKnowledgeBaseHandler;
    private DomainTagUserDefineKnowledgeBaseHandler domainKnowledgeBaseHandler;
    private AppTagUserDefineKnowledgeBaseHandler appKnowledgeBaseHandler;
    private RuleKnowledgeBaseHandler ruleKnowledgeBaseHandler;

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {
        option = udfContext.getParameters().get("option").toString();
        super.open(runtimeContext, udfContext);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Event evaluate(Event event) {
        if (event.getExtractedFields().get(lookupFieldName) != null && event.getExtractedFields().get(lookupFieldName).toString().length() != 0) {
            String lookupValue = event.getExtractedFields().get(lookupFieldName).toString();
            List<String> tags = new ArrayList<>();
            RuleMetadata ruleMetadata = new RuleMetadata();
            switch (option) {
                case "IP_TO_TAG":
                    List<AbstractMultipleKnowledgeBaseHandler.Node> ipNodes = ipKnowledgeBaseHandler.lookup(lookupValue);
                    ipNodes.forEach(node -> {
                        tags.add(node.getTag());
                        List<RuleKnowledgeBaseHandler.Rule> rules = ruleKnowledgeBaseHandler.lookupByKbId(node.getKbId());
                        if (rules != null) {
                            rules.forEach(rule -> ruleMetadata.addRule(rule.getRuleId(), IocType.IP.getType()));
                        }
                    });
                    break;
                case "DOMAIN_TO_TAG":
                    List<AbstractMultipleKnowledgeBaseHandler.Node> domainNodes = domainKnowledgeBaseHandler.lookup(lookupValue);
                    domainNodes.forEach(node -> {
                        tags.add(node.getTag());
                        List<RuleKnowledgeBaseHandler.Rule> rules = ruleKnowledgeBaseHandler.lookupByKbId(node.getKbId());
                        if (rules != null) {
                            rules.forEach(rule -> ruleMetadata.addRule(rule.getRuleId(), IocType.DOMAIN.getType()));
                        }
                    });
                    break;
                case "APP_TO_TAG":
                    List<AbstractMultipleKnowledgeBaseHandler.Node> appNodes = appKnowledgeBaseHandler.lookup(lookupValue);
                    appNodes.forEach(node -> {
                        tags.add(node.getTag());
                        List<RuleKnowledgeBaseHandler.Rule> rules = ruleKnowledgeBaseHandler.lookupByKbId(node.getKbId());
                        if (rules != null) {
                            rules.forEach(rule -> ruleMetadata.addRule(rule.getRuleId(), IocType.APP.getType()));
                        }
                    });
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
