package com.geedgenetworks.core.udf.cn;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.udf.UDFContext;
import com.geedgenetworks.core.udf.knowlegdebase.KnowledgeBaseUpdateJob;
import com.geedgenetworks.core.udf.knowlegdebase.handler.IntelligenceIndicatorKnowledgeBaseHandler;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/4/7 14:15
 */
public class IntelligenceIndicatorLookup extends AbstractKnowledgeUDF {

    private static final Logger logger = LoggerFactory.getLogger(IntelligenceIndicatorLookup.class);

    private String option;

    private IntelligenceIndicatorKnowledgeBaseHandler knowledgeBaseHandler;

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {
        super.open(runtimeContext, udfContext);
        option = udfContext.getParameters().get("option").toString();
    }

    @Override
    public Event evaluate(Event event) {
        if (event.getExtractedFields().get(lookupFieldName) != null && event.getExtractedFields().get(lookupFieldName).toString().length() != 0) {
            String lookupValue = event.getExtractedFields().get(lookupFieldName).toString();
            switch (option) {
                case "IP_TO_TAG":
                    List<String> ipTags = knowledgeBaseHandler.lookupByIp(lookupValue);
                    if (event.getExtractedFields().get(outputFieldName) != null && event.getExtractedFields().get(outputFieldName) instanceof List) {
                        ((List<String>) event.getExtractedFields().get(outputFieldName)).addAll(ipTags);
                    } else {
                        event.getExtractedFields().put(outputFieldName, ipTags);
                    }
                    break;
                case "DOMAIN_TO_TAG":
                    List<String> domainTags = knowledgeBaseHandler.lookupByDomain(lookupValue);
                    if (event.getExtractedFields().get(outputFieldName) != null && event.getExtractedFields().get(outputFieldName) instanceof List) {
                        ((List<String>) event.getExtractedFields().get(outputFieldName)).addAll(domainTags);
                    } else {
                        event.getExtractedFields().put(outputFieldName, domainTags);
                    }
                    break;
                default:
                    logger.error("unknown option :" + option);
                    break;
            }
        }
        return event;
    }

    @Override
    public String functionName() {
        return "CN_INTELLIGENCE_INDICATOR_LOOKUP";
    }

    @Override
    public void close() {

    }

    @Override
    protected void registerKnowledges() {
        knowledgeBaseHandler = IntelligenceIndicatorKnowledgeBaseHandler.getInstance();
        KnowledgeBaseUpdateJob.registerKnowledgeBase(knowledgeBaseHandler, knowledgeBaseConfigs.get(0));
    }
}
