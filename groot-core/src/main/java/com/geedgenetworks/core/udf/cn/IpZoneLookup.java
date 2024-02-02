package com.geedgenetworks.core.udf.cn;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.core.udf.knowlegdebase.handler.InternalIpKnowledgeBaseHandler;
import com.geedgenetworks.core.udf.knowlegdebase.KnowledgeBaseUpdateJob;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/1/22 10:19
 */
public class IpZoneLookup extends AbstractKnowledgeUDF {

    private InternalIpKnowledgeBaseHandler knowledgeBaseHandler;

    @Override
    public Event evaluate(Event event) {
        if (event.getExtractedFields().get(lookupFieldName) != null && event.getExtractedFields().get(lookupFieldName).toString().length() != 0) {
            String ip = event.getExtractedFields().get(lookupFieldName).toString();
            String zone = knowledgeBaseHandler.isInternal(ip) ? "internal" : "external";
            event.getExtractedFields().put(outputFieldName, zone);
        }
        return event;
    }

    @Override
    public String functionName() {
        return "CN_IP_ZONE_LOOKUP";
    }

    @Override
    public void close() {

    }

    @Override
    protected void registerKnowledges() {
        knowledgeBaseHandler = InternalIpKnowledgeBaseHandler.getInstance();
        KnowledgeBaseUpdateJob.registerKnowledgeBase(knowledgeBaseHandler, knowledgeBaseConfigs.get(0));
    }
}
