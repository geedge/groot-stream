package com.geedgenetworks.core.udf.cn;

import com.geedgenetworks.core.udf.knowlegdebase.KnowledgeBaseUpdateJob;
import com.geedgenetworks.core.udf.knowlegdebase.handler.IocDarkwebKnowledgeBaseHandler;
import com.geedgenetworks.core.udf.knowlegdebase.handler.RuleKnowledgeBaseHandler;
import com.geedgenetworks.spi.common.udf.UDFContext;
import com.geedgenetworks.spi.table.event.Event;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/1/22 14:25
 */
public class AnonymityLookup extends AbstractKnowledgeWithRuleScalarFunction {

    private static final Logger logger = LoggerFactory.getLogger(AnonymityLookup.class);

    private String option;

    private IocDarkwebKnowledgeBaseHandler knowledgeBaseHandler;

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
            lookUpExistCounter.inc();
            String lookupValue = event.getExtractedFields().get(lookupFieldName).toString();
            RuleMetadata ruleMetadata = new RuleMetadata();
            switch (option) {
                case "IP_TO_NODE_TYPE":
                    String ipNodeType = knowledgeBaseHandler.lookupByIp(lookupValue);
                    if (ipNodeType != null) {
                        hitCounter.inc();
                        event.getExtractedFields().put(outputFieldName, ipNodeType);
                        RuleKnowledgeBaseHandler.Rule ipRule = ruleKnowledgeBaseHandler.lookupByName(ipNodeType);
                        if (ipRule != null) {
                            ruleHitCounter.inc();
                            ruleMetadata.addRule(ipRule.getRuleId(), IocType.IP.getType());
                        }
                    }

                    break;
                case "DOMAIN_TO_NODE_TYPE":
                    String domainNodeType = knowledgeBaseHandler.lookupByDomain(lookupValue);
                    if (domainNodeType != null) {
                        hitCounter.inc();
                        event.getExtractedFields().put(outputFieldName, domainNodeType);
                        RuleKnowledgeBaseHandler.Rule domainRule = ruleKnowledgeBaseHandler.lookupByName(domainNodeType);
                        if (domainRule != null) {
                            ruleHitCounter.inc();
                            ruleMetadata.addRule(domainRule.getRuleId(), IocType.DOMAIN.getType());
                        }
                    }
                    break;
                default:
                    logger.error("unknown option: " + option);
                    break;
            }
            (((ArrayList<Long>) (event.getExtractedFields().computeIfAbsent(internalRuleIdListFieldName, k -> new ArrayList<Long>())))).addAll(ruleMetadata.getRuleIds());
            (((ArrayList<String>) (event.getExtractedFields().computeIfAbsent(internalIocTypeListFieldName, k -> new ArrayList<String>())))).addAll(ruleMetadata.getIocTypes());
        }
        return event;
    }

    @Override
    public String functionName() {
        return "CN_ANONYMITY_LOOKUP";
    }

    @Override
    public void close() {

    }

    @Override
    protected void registerKnowledges() {
        ruleKnowledgeBaseHandler = RuleKnowledgeBaseHandler.getInstance();
        KnowledgeBaseUpdateJob.registerKnowledgeBase(ruleKnowledgeBaseHandler, ruleConfigs.get(0));

        knowledgeBaseHandler = IocDarkwebKnowledgeBaseHandler.getInstance();
        KnowledgeBaseUpdateJob.registerKnowledgeBase(knowledgeBaseHandler, knowledgeBaseConfigs.get(0));
    }
}
