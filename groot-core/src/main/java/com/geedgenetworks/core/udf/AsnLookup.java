package com.geedgenetworks.core.udf;

import com.alibaba.fastjson2.JSON;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.config.CommonConfig;
import com.geedgenetworks.common.config.KnowledgeBaseConfig;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.udf.ScalarFunction;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.udf.UDFContext;
import com.geedgenetworks.core.udf.knowlegdebase.handler.AsnKnowledgeBaseHandler;

import com.geedgenetworks.core.udf.knowlegdebase.KnowledgeBaseUpdateJob;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

@Slf4j
public class AsnLookup implements ScalarFunction {

    private String kbName;
    private String option;
    private String lookupFieldName;
    private String outputFieldName;

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {

        checkUdfContext(udfContext);
        this.kbName = udfContext.getParameters().get("kb_name").toString();
        this.option = udfContext.getParameters().get("option").toString();
        Configuration configuration = (Configuration) runtimeContext
                .getExecutionConfig().getGlobalJobParameters();
        CommonConfig commonConfig = JSON.parseObject(configuration.toMap().get(Constants.SYSPROP_GROOTSTREAM_CONFIG), CommonConfig.class);
        for (KnowledgeBaseConfig knowledgeBaseConfig : commonConfig.getKnowledgeBaseConfig()) {
            if (kbName.equals(knowledgeBaseConfig.getName())) {
                KnowledgeBaseUpdateJob.registerKnowledgeBase(AsnKnowledgeBaseHandler.getInstance(), knowledgeBaseConfig);
                break;
            }
        }
        if (AsnKnowledgeBaseHandler.getKbNameWithKbConfigAndFileMetas().containsKey(kbName) && AsnKnowledgeBaseHandler.getKbNameWithKbConfigAndFileMetas().get(kbName).getIpLookupV2() != null) {
            log.debug("Init AsnKnowledgeBase success ");
        } else {
            log.error("AsnLookup init KnowledgeBase error ");
        }
        this.lookupFieldName = udfContext.getLookup_fields().get(0);
        this.outputFieldName = udfContext.getOutput_fields().get(0);
        AsnKnowledgeBaseHandler.increment();
        log.warn("AsnKnowledgeBaseHandlerCount "+AsnKnowledgeBaseHandler.getCount());
    }


    @Override
    public Event evaluate(Event event) {

        Object valueObj = event.getExtractedFields().get(lookupFieldName);
        if (valueObj!=null) {
            String asn = AsnKnowledgeBaseHandler.lookUp(kbName, option, valueObj.toString());
            if (!asn.isEmpty()) {
                event.getExtractedFields().put(outputFieldName, asn);
            }
        }

        return event;
    }

    @Override
    public String functionName() {
        return "ASN_LOOKUP";
    }

    @Override
    public void close() {
        AsnKnowledgeBaseHandler.decrement();
        if (AsnKnowledgeBaseHandler.getCount()<=0) {
            KnowledgeBaseUpdateJob.removeKnowledgeBase(kbName);
            log.warn("AsnKnowledgeBaseHandler removeKnowledgeBase "+kbName);
        }
    }

    private void checkUdfContext(UDFContext udfContext) {

        if (udfContext.getLookup_fields() == null || udfContext.getOutput_fields() == null || udfContext.getParameters() == null) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Missing required parameters");
        }

        if (udfContext.getLookup_fields().size() != 1) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function lookup fields only support 1 value");
        }
        if (udfContext.getOutput_fields().size() != 1) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function output fields only support 1 value");
        }

        if (!udfContext.getParameters().containsKey("kb_name")) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "parameters need containkey kb_name");
        }
        if (!udfContext.getParameters().containsKey("option")) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "parameters need containkey option");
        } else {
            if (!udfContext.getParameters().get("option").toString().equals("IP_TO_ASN")) {
                throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "parameters option value is not correct");
            }
        }
    }

}
