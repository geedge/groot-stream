package com.geedgenetworks.core.udf.cn;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.core.udf.knowlegdebase.KnowledgeBaseUpdateJob;
import com.geedgenetworks.core.udf.knowlegdebase.handler.InternalIpKnowledgeBaseHandler;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/1/22 10:19
 * @description: CN_IP_ZONE_LOOKUP 如果知识库配置不为空,则支持根据知识库判断IP是否为内部IP
 * 如果知识库配置为空,则只根据内网网段判断是否为内部IP
 * 该函数优先级应该置于地理位置判断和flag字段判断之后
 */
public class IpZoneLookup extends AbstractKnowledgeScalarFunction {

    private InternalIpKnowledgeBaseHandler knowledgeBaseHandler;

    @Override
    public Event evaluate(Event event) {
        if (event.getExtractedFields().get(lookupFieldName) != null && event.getExtractedFields().get(lookupFieldName).toString().length() != 0) {
            lookUpExistCounter.inc();
            String ip = event.getExtractedFields().get(lookupFieldName).toString();
            if (knowledgeBaseHandler.isInternal(ip)) {
                event.getExtractedFields().put(outputFieldName, "internal");
            } else {
                if (knowledgeBaseConfigs.size() != 0) {
                    event.getExtractedFields().put(outputFieldName, "external");
                }
            }
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
        if (knowledgeBaseConfigs.size() == 0) {
            return;
        }
        KnowledgeBaseUpdateJob.registerKnowledgeBase(knowledgeBaseHandler, knowledgeBaseConfigs.get(0));
    }
}
