package com.geedgenetworks.core.udf.cn;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.core.utils.KnowlegdeBase.Handler.cn.IdcRenterKnowledgeBaseHandler;
import com.geedgenetworks.core.utils.KnowlegdeBase.KnowledgeBaseUpdateJob;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/1/18 11:45
 */
public class IdcRenterLookup extends AbstractKnowledgeUDF {

    private IdcRenterKnowledgeBaseHandler knowledgeBaseHandler;

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
        return "CN_IDC_RENTER_LOOKUP";
    }

    @Override
    public void close() {

    }

    @Override
    protected void registerKnowledges() {
        knowledgeBaseHandler = IdcRenterKnowledgeBaseHandler.getInstance();
        KnowledgeBaseUpdateJob.registerKnowledgeBase(knowledgeBaseHandler, knowledgeBaseConfigs.get(0));
    }
}
