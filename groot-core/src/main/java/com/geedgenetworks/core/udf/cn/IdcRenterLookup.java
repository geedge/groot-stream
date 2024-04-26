package com.geedgenetworks.core.udf.cn;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.core.udf.knowlegdebase.KnowledgeBaseUpdateJob;
import com.geedgenetworks.core.udf.knowlegdebase.handler.IdcRenterKnowledgeBaseHandler;

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
            lookUpExistCounter.inc();
            String idcRenter = knowledgeBaseHandler.lookup(
                    event.getExtractedFields().get(lookupFieldName).toString()
            );
            if (idcRenter != null) {
                hitCounter.inc();
                event.getExtractedFields().put(outputFieldName, idcRenter);
            }
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
