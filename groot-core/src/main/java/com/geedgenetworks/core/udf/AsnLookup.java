package com.geedgenetworks.core.udf;

import com.alibaba.fastjson.JSON;
import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.config.EngineConfig;
import com.geedgenetworks.common.config.KnowledgeConfig;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;
import com.geedgenetworks.core.utils.KnowlegdeBase.AsnKnowledgeBase;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.geedgenetworks.core.utils.KnowlegdeBase.KnowledgeBaseSchedule;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

public class AsnLookup implements UDF {

    private UDFContext udfContext;
    private static final Log logger = LogFactory.get();
    private String vender;
    private String option;

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext)  {

        checkUdfContext(udfContext);
        this.udfContext = udfContext;
        this.vender = udfContext.getParameters().get("vendor_id").toString();
        this.option = udfContext.getParameters().get("option").toString();
        Configuration configuration = (Configuration) runtimeContext
                .getExecutionConfig().getGlobalJobParameters();
        EngineConfig engineConfig = JSON.parseObject(configuration.toMap().get(Constants.SYSPROP_GROOTSTREAM_CONFIG), EngineConfig.class);
        for(KnowledgeConfig knowledgeConfig : engineConfig.getKnowledgeBaseConfig()){
            if(vender.equals(knowledgeConfig.getName())){
                KnowledgeBaseSchedule.initKnowledgeBase(knowledgeConfig);
            }
            else {
                throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The param vendor_id value is not correct");
            }
        }

    }


    @Override
    public Event evaluate(Event event) {


        switch (option){
            case "IP_TO_ASN":
                event.getExtractedFields()
                        .put(
                                udfContext.getOutput_fields().get(0),
                                AsnKnowledgeBase.getVenderWithAsnLookup()
                                        .get(vender)
                                        .asnLookup(
                                                event.getExtractedFields()
                                                        .get(
                                                                udfContext
                                                                        .getLookup_fields()
                                                                        .get(0))
                                                        .toString()));
                    break;
            default:
                    break;
        }
        return event;
    }

    @Override
    public String functionName() {
        return "ASN_LOOKUP";
    }

    @Override
    public void close() {

    }

    private void checkUdfContext(UDFContext udfContext) {
        if(udfContext.getLookup_fields().size() != 1){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function lookup fields only support 1 value");
        }
        if(udfContext.getOutput_fields().size() != 1){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function output fields only support 1 value");
        }
        if(udfContext.getParameters() == null){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function needs parameters");
        }
        if(!udfContext.getParameters().containsKey("vendor_id")){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "parameters need containkey vendor_id");
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
