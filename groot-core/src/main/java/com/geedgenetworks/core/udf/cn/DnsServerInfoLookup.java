package com.geedgenetworks.core.udf.cn;

import com.geedgenetworks.core.udf.knowlegdebase.KnowledgeBaseUpdateJob;
import com.geedgenetworks.core.udf.knowlegdebase.handler.DnsServerInfoKnowledgeBaseHandler;
import com.geedgenetworks.api.connector.event.Event;

import java.util.List;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/1/18 16:21
 */
public class DnsServerInfoLookup extends AbstractKnowledgeScalarFunction {

    private DnsServerInfoKnowledgeBaseHandler knowledgeBaseHandler;

    @Override
    public Event evaluate(Event event) {
        if (event.getExtractedFields().get(lookupFieldName) != null && event.getExtractedFields().get(lookupFieldName).toString().length() != 0) {
            lookUpExistCounter.inc();
            List<String> dnsServerRoleList = knowledgeBaseHandler.lookup(
                    event.getExtractedFields().get(lookupFieldName).toString()
            );
            if (dnsServerRoleList != null) {
                hitCounter.inc();
                event.getExtractedFields().put(outputFieldName, dnsServerRoleList);
            }
        }
        return event;
    }

    @Override
    public String functionName() {
        return "CN_DNS_SERVER_LOOKUP";
    }

    @Override
    public void close() {

    }

    @Override
    protected void registerKnowledges() {
        knowledgeBaseHandler = DnsServerInfoKnowledgeBaseHandler.getInstance();
        KnowledgeBaseUpdateJob.registerKnowledgeBase(knowledgeBaseHandler, knowledgeBaseConfigs.get(0));
    }
}
