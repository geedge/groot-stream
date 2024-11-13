package com.geedgenetworks.core.udf.cn;

import com.geedgenetworks.core.udf.knowlegdebase.KnowledgeBaseUpdateJob;
import com.geedgenetworks.core.udf.knowlegdebase.handler.FqdnIcpKnowledgeBaseHandler;
import com.geedgenetworks.api.event.Event;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/1/22 10:27
 */
public class IcpLookup extends AbstractKnowledgeScalarFunction {

    private FqdnIcpKnowledgeBaseHandler knowledgeBaseHandler;

    @Override
    public Event evaluate(Event event) {
        if (event.getExtractedFields().get(lookupFieldName) != null && event.getExtractedFields().get(lookupFieldName).toString().length() != 0) {
            lookUpExistCounter.inc();
            String icpCompanyName = knowledgeBaseHandler.lookup(
                    event.getExtractedFields().get(lookupFieldName).toString()
            );
            if (icpCompanyName != null) {
                hitCounter.inc();
                event.getExtractedFields().put(outputFieldName, icpCompanyName);
            }
        }
        return event;
    }

    @Override
    public String functionName() {
        return "CN_ICP_LOOKUP";
    }

    @Override
    public void close() {

    }

    @Override
    protected void registerKnowledges() {
        knowledgeBaseHandler = FqdnIcpKnowledgeBaseHandler.getInstance();
        KnowledgeBaseUpdateJob.registerKnowledgeBase(knowledgeBaseHandler, knowledgeBaseConfigs.get(0));
    }
}
