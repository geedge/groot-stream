package com.geedgenetworks.core.udf.cn;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.core.udf.knowlegdeBase.handler.FqdnWhoisKnowledgeBaseHandler;
import com.geedgenetworks.core.udf.knowlegdeBase.KnowledgeBaseUpdateJob;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/1/22 13:59
 */
public class FqdnWhoisLookup extends AbstractKnowledgeUDF {

    private FqdnWhoisKnowledgeBaseHandler knowledgeBaseHandler;

    @Override
    public Event evaluate(Event event) {
        if (event.getExtractedFields().get(lookupFieldName) != null && event.getExtractedFields().get(lookupFieldName).toString().length() != 0) {
            event.getExtractedFields().put(
                    outputFieldName,
                    knowledgeBaseHandler.lookup(
                            event.getExtractedFields().get(lookupFieldName).toString()
                    )
            );
        }
        return event;
    }

    @Override
    public String functionName() {
        return "CN_FQDN_WHOIS_LOOKUP";
    }

    @Override
    public void close() {

    }

    @Override
    protected void registerKnowledges() {
        knowledgeBaseHandler = FqdnWhoisKnowledgeBaseHandler.getInstance();
        KnowledgeBaseUpdateJob.registerKnowledgeBase(knowledgeBaseHandler, knowledgeBaseConfigs.get(0));
    }
}
