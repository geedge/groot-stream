package com.geedgenetworks.core.udf;

import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;
import com.geedgenetworks.core.utils.KnowlegdeBase.AsnKnowledgeBase;

import com.geedgenetworks.core.utils.KnowlegdeBase.KnowledgeBaseUpdateJob;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;

@Slf4j
public class AsnLookup implements UDF {

    private UDFContext udfContext;
    private String vender;
    private String option;

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext)  {

        checkUdfContext(udfContext);
        this.udfContext = udfContext;
        this.vender = udfContext.getParameters().get("vendor_id").toString();
        this.option = udfContext.getParameters().get("option").toString();
        KnowledgeBaseUpdateJob.initKnowledgeBase(vender,AsnKnowledgeBase.getInstance(), runtimeContext);

        if(AsnKnowledgeBase.getVenderWithAsnLookup()!=null && AsnKnowledgeBase.getVenderWithAsnLookup().containsKey(vender)){
            log.debug("Init AsnKnowledgeBase success ");
        }
        else {
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Init AsnKnowledgeBase error ");
        }
    }


    @Override
    public Event evaluate(Event event) {

        if(AsnKnowledgeBase.getVenderWithAsnLookup()!=null && AsnKnowledgeBase.getVenderWithAsnLookup().containsKey(vender)){

            if(event.getExtractedFields().containsKey(udfContext.getLookup_fields().get(0))){
                switch (option) {
                    case "IP_TO_ASN":
                        String asn = AsnKnowledgeBase.getVenderWithAsnLookup()
                                .get(vender)
                                    .asnLookup(event.getExtractedFields().get(udfContext.getLookup_fields().get(0)).toString());
                        if(!asn.isEmpty()) {
                            event.getExtractedFields()
                                    .put(udfContext.getOutput_fields().get(0), asn);
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
