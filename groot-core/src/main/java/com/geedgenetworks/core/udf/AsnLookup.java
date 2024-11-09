package com.geedgenetworks.core.udf;

import com.alibaba.fastjson2.JSON;
import com.geedgenetworks.common.config.Constants;
import com.geedgenetworks.common.config.*;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.core.udf.knowlegdebase.handler.AsnKnowledgeBaseHandler;

import com.geedgenetworks.core.udf.knowlegdebase.KnowledgeBaseUpdateJob;
import com.geedgenetworks.spi.common.udf.ScalarFunction;
import com.geedgenetworks.spi.common.udf.UDFContext;
import com.geedgenetworks.spi.configuration.CheckUDFContextUtil;
import com.geedgenetworks.spi.configuration.UDFContextConfigOptions;
import com.geedgenetworks.spi.table.event.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

@Slf4j
public class AsnLookup implements ScalarFunction {

    private String kbName;
    private String option;
    private String lookupFieldName;
    private String outputFieldName;


    enum Option {
        IP_TO_ASN;

        public static boolean isValid(String option) {
            try {
                Option.valueOf(option);
                return true;
            } catch (IllegalArgumentException e) {
                return false;
            }
        }
    }
    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {

        checkConfig(udfContext);
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
        this.lookupFieldName = udfContext.getLookupFields().get(0);
        this.outputFieldName = udfContext.getOutputFields().get(0);
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

    @Override
    public void checkConfig(UDFContext udfContext) {
        CheckResult result = CheckUDFContextUtil.checkAllExists(udfContext,
                UDFContextConfigOptions.LOOKUP_FIELDS.key(),
                UDFContextConfigOptions.OUTPUT_FIELDS.key(),
                UDFContextConfigOptions.PARAMETERS.key());

        if (!result.isSuccess()) {
            throw new GrootStreamRuntimeException(CommonErrorCode.CONFIG_VALIDATION_FAILED, result.getMsg());
        }

        result = CheckUDFContextUtil.checkCollectionSingleItemExists(udfContext, UDFContextConfigOptions.LOOKUP_FIELDS.key());
        if (!result.isSuccess()) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, result.getMsg());
        }

        result = CheckUDFContextUtil.checkCollectionSingleItemExists(udfContext, UDFContextConfigOptions.OUTPUT_FIELDS.key());
        if (!result.isSuccess()) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, result.getMsg());
        }

        result = CheckUDFContextUtil.checkParametersContainsKeys(udfContext, UDFContextConfigOptions.PARAMETERS_KB_NAME.key(), UDFContextConfigOptions.PARAMETERS_OPTION.key());
        if (!result.isSuccess()) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, result.getMsg());
        }

        String optionValue = udfContext.getParameters().get(UDFContextConfigOptions.PARAMETERS_OPTION.key()).toString();
        if (!Option.isValid(optionValue)) {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, String.format( "UDF: %s, [%s] Option value is not correct.",
                    udfContext.getFunction(), UDFContextConfigOptions.PARAMETERS.key()));
        }

    }

}
