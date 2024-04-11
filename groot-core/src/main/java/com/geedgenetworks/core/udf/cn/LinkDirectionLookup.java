package com.geedgenetworks.core.udf.cn;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.core.udf.knowlegdebase.KnowledgeBaseUpdateJob;
import com.geedgenetworks.core.udf.knowlegdebase.handler.LinkDirectionKnowledgeBaseHandler;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/1/17 14:56
 */
public class LinkDirectionLookup extends AbstractKnowledgeUDF {

    private LinkDirectionKnowledgeBaseHandler knowledgeBaseHandler;

    @Override
    public Event evaluate(Event event) {
        if (event.getExtractedFields().get(lookupFieldName) != null && event.getExtractedFields().get(lookupFieldName).toString().length() != 0) {
            lookUpExistCounter.inc();
            String peerCity = knowledgeBaseHandler.lookup(
                    event.getExtractedFields().get(lookupFieldName).toString()
            );
            if (peerCity != null) {
                hitCounter.inc();
                event.getExtractedFields().put(outputFieldName, peerCity);
            }
        }
        return event;
    }

    @Override
    public String functionName() {
        return "CN_LINK_DIRECTION_LOOKUP";
    }

    @Override
    public void close() {

    }

    @Override
    protected void registerKnowledges() {
        knowledgeBaseHandler = LinkDirectionKnowledgeBaseHandler.getInstance();
        KnowledgeBaseUpdateJob.registerKnowledgeBase(knowledgeBaseHandler, knowledgeBaseConfigs.get(0));
    }
}
