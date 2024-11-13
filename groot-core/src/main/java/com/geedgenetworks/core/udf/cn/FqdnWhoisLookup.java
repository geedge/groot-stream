package com.geedgenetworks.core.udf.cn;

import com.geedgenetworks.core.udf.knowlegdebase.KnowledgeBaseUpdateJob;
import com.geedgenetworks.core.udf.knowlegdebase.handler.FqdnWhoisKnowledgeBaseHandler;
import com.geedgenetworks.api.event.Event;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/1/22 13:59
 */
public class FqdnWhoisLookup extends AbstractKnowledgeScalarFunction {

    private FqdnWhoisKnowledgeBaseHandler knowledgeBaseHandler;

    @Override
    public Event evaluate(Event event) {
        if (event.getExtractedFields().get(lookupFieldName) != null && event.getExtractedFields().get(lookupFieldName).toString().length() != 0) {
            lookUpExistCounter.inc();
            String whoisRegistrantOrg = knowledgeBaseHandler.lookup(
                    event.getExtractedFields().get(lookupFieldName).toString()
            );
            if (whoisRegistrantOrg != null) {
                hitCounter.inc();
                event.getExtractedFields().put(outputFieldName, whoisRegistrantOrg);
            }
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
