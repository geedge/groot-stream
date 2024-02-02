package com.geedgenetworks.core.udf.cn;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.udf.UDFContext;
import com.geedgenetworks.core.udf.knowlegdebase.handler.DomainVpnKnowledgeBaseHandler;
import com.geedgenetworks.core.udf.knowlegdebase.handler.IpVpnKnowledgeBaseHandler;
import com.geedgenetworks.core.udf.knowlegdebase.KnowledgeBaseUpdateJob;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/1/19 10:40
 */
public class VpnLookup extends AbstractKnowledgeUDF {

    private static final Logger logger = LoggerFactory.getLogger(VpnLookup.class);

    private String option;

    private IpVpnKnowledgeBaseHandler ipVpnKnowledgeBaseHandler;

    private DomainVpnKnowledgeBaseHandler domainVpnKnowledgeBaseHandler;

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {
        option = udfContext.getParameters().get("option").toString();
        super.open(runtimeContext, udfContext);
    }

    @Override
    public Event evaluate(Event event) {
        if (event.getExtractedFields().get(lookupFieldName) != null && event.getExtractedFields().get(lookupFieldName).toString().length() != 0) {
            String lookup = event.getExtractedFields().get(lookupFieldName).toString();
            String result = null;
            switch (option) {
                case "IP_TO_VPN":
                    result = ipVpnKnowledgeBaseHandler.lookup(lookup);
                    break;
                case "DOMAIN_TO_VPN":
                    result = domainVpnKnowledgeBaseHandler.lookup(lookup);
                    break;
                default:
                    logger.error("unknown option: " + option);
                    break;
            }
            event.getExtractedFields().put(outputFieldName, result);
        }
        return event;
    }

    @Override
    public String functionName() {
        return "CN_VPN_LOOKUP";
    }

    @Override
    public void close() {

    }

    @Override
    protected void registerKnowledges() {
        switch (option) {
            case "IP_TO_VPN":
                ipVpnKnowledgeBaseHandler = IpVpnKnowledgeBaseHandler.getInstance();
                KnowledgeBaseUpdateJob.registerKnowledgeBase(ipVpnKnowledgeBaseHandler, knowledgeBaseConfigs.get(0));
                break;
            case "DOMAIN_TO_VPN":
                domainVpnKnowledgeBaseHandler = DomainVpnKnowledgeBaseHandler.getInstance();
                KnowledgeBaseUpdateJob.registerKnowledgeBase(domainVpnKnowledgeBaseHandler, knowledgeBaseConfigs.get(0));
                break;
            default:
                break;
        }

    }
}
