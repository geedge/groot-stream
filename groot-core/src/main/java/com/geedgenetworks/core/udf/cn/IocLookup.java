package com.geedgenetworks.core.udf.cn;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.udf.UDFContext;
import com.geedgenetworks.core.udf.knowlegdeBase.handler.IocMalwareKnowledgeBaseHandler;
import com.geedgenetworks.core.udf.knowlegdeBase.handler.RuleKnowledgeBaseHandler;
import com.geedgenetworks.core.udf.knowlegdeBase.KnowledgeBaseUpdateJob;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/1/22 14:45
 */
public class IocLookup extends AbstractKnowledgeWithRuleUDF {

    private static final Logger logger = LoggerFactory.getLogger(IocLookup.class);

    private String option;

    private IocMalwareKnowledgeBaseHandler knowledgeBaseHandler;

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
            RuleMetadata ruleMetadata = new RuleMetadata();
            switch (option) {
                case "IP_TO_MALWARE":
                    String ipMalware = knowledgeBaseHandler.lookupByIp(lookupValue);
                    event.getExtractedFields().put(outputFieldName, ipMalware);
                    RuleKnowledgeBaseHandler.Rule ipRule = ruleKnowledgeBaseHandler.lookupByName(ipMalware);
                    if (ipRule != null) {
                        ruleMetadata.addRule(ipRule.getRuleId(), IocType.IP.getType());
                    }
                    break;
                case "DOMAIN_TO_MALWARE":
                    String domainMalware = knowledgeBaseHandler.lookupByDomain(lookupValue);
                    event.getExtractedFields().put(outputFieldName, domainMalware);
                    RuleKnowledgeBaseHandler.Rule domainRule = ruleKnowledgeBaseHandler.lookupByName(domainMalware);
                    if (domainRule != null) {
                        ruleMetadata.addRule(domainRule.getRuleId(), IocType.DOMAIN.getType());
                    }
                    break;
                case "HTTP_URL_TO_MALWARE":
                    String urlMalware = knowledgeBaseHandler.lookupByUrl(lookupValue);
                    event.getExtractedFields().put(outputFieldName, urlMalware);
                    RuleKnowledgeBaseHandler.Rule urlRule = ruleKnowledgeBaseHandler.lookupByName(urlMalware);
                    if (urlRule != null) {
                        ruleMetadata.addRule(urlRule.getRuleId(), IocType.DOMAIN.getType());
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
        return "CN_IOC_LOOKUP";
    }

    @Override
    public void close() {

    }

    @Override
    protected void registerKnowledges() {
        ruleKnowledgeBaseHandler = RuleKnowledgeBaseHandler.getInstance();
        KnowledgeBaseUpdateJob.registerKnowledgeBase(ruleKnowledgeBaseHandler, ruleConfigs.get(0));

        knowledgeBaseHandler = IocMalwareKnowledgeBaseHandler.getInstance();
        KnowledgeBaseUpdateJob.registerKnowledgeBase(knowledgeBaseHandler, knowledgeBaseConfigs.get(0));
    }
}
