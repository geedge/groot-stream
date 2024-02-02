package com.geedgenetworks.core.udf.cn;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.core.udf.knowlegdebase.handler.FqdnIcpKnowledgeBaseHandler;
import com.geedgenetworks.core.udf.knowlegdebase.KnowledgeBaseUpdateJob;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/1/22 10:27
 */
public class IcpLookup extends AbstractKnowledgeUDF {

    private FqdnIcpKnowledgeBaseHandler knowledgeBaseHandler;

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
