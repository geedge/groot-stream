package com.geedgenetworks.core.udf;

import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.config.CommonConfig;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;
import com.geedgenetworks.core.utils.KnowlegdeBase.AsnKnowledgeBase;

import com.geedgenetworks.core.utils.KnowlegdeBase.KnowledgeBaseUpdateJob;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

@Slf4j
public class AsnLookup implements UDF {

    private String kbName;
    private String option;
    private String lookupFieldName;
    private String outputFieldName;

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext)  {

        checkUdfContext(udfContext);
        this.kbName = udfContext.getParameters().get("kb_name").toString();
        this.option = udfContext.getParameters().get("option").toString();
        Configuration configuration = (Configuration) runtimeContext
                .getExecutionConfig().getGlobalJobParameters();
        CommonConfig commonConfig = com.alibaba.fastjson.JSON.parseObject(configuration.toMap().get(Constants.SYSPROP_GROOTSTREAM_CONFIG), CommonConfig.class);

        KnowledgeBaseUpdateJob.initKnowledgeBase(kbName,AsnKnowledgeBase.getInstance(), commonConfig);

        if(AsnKnowledgeBase.getVenderWithAsnLookup()!=null && AsnKnowledgeBase.getVenderWithAsnLookup().containsKey(kbName)){
            log.debug("Init AsnKnowledgeBase success ");
        }
        else {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Init AsnKnowledgeBase error ");
        }
        this.lookupFieldName = udfContext.getLookup_fields().get(0);
        this.outputFieldName = udfContext.getOutput_fields().get(0);
    }


    @Override
    public Event evaluate(Event event) {

        if(AsnKnowledgeBase.getVenderWithAsnLookup()!=null && AsnKnowledgeBase.getVenderWithAsnLookup().containsKey(kbName)){

            if(event.getExtractedFields().containsKey(lookupFieldName)){
                switch (option) {
                    case "IP_TO_ASN":
                        String asn = AsnKnowledgeBase.getVenderWithAsnLookup()
                                .get(kbName)
                                    .asnLookup(event.getExtractedFields().get(lookupFieldName).toString());
                        if(!asn.isEmpty()) {
                            event.getExtractedFields()
                                    .put(outputFieldName, asn);
                        }
                        break;
                    default:
                        break;
                }
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

        KnowledgeBaseUpdateJob.destroyKnowledgeBase();
    }

    private void checkUdfContext(UDFContext udfContext) {

        if(udfContext.getLookup_fields()==null || udfContext.getOutput_fields()==null || udfContext.getParameters() == null){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Missing required parameters");
        }

        if(udfContext.getLookup_fields().size() != 1){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function lookup fields only support 1 value");
        }
        if(udfContext.getOutput_fields().size() != 1){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function output fields only support 1 value");
        }

        if(!udfContext.getParameters().containsKey("kb_name")){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "parameters need containkey kb_name");
        }
        if(!udfContext.getParameters().containsKey("option")){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "parameters need containkey option");
        }
        else{
            if(!udfContext.getParameters().get("option").toString().equals("IP_TO_ASN")){
                throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "parameters option value is not correct");
            }
        }
    }

}
