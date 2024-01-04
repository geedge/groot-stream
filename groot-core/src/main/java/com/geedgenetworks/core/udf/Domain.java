package com.geedgenetworks.core.udf;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;
import com.geedgenetworks.utils.FormatUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.util.List;

@Slf4j
public class Domain implements UDF {


    private String option;
    private List<String> lookupFields;
    private String outputFieldName;
    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {

        if(udfContext.getLookup_fields()==null || udfContext.getOutput_fields()==null || udfContext.getParameters() == null){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "Missing required parameters");
        }
        if(udfContext.getLookup_fields().isEmpty()){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function lookup field is not empty");
        }
        if(udfContext.getOutput_fields().size() != 1){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "The function output fields only support 1 value");
        }
        if(!udfContext.getParameters().containsKey("option")){
            throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "parameters need containkey option");
        }
        else{
            if(!udfContext.getParameters().get("option").toString().equals("TOP_LEVEL_DOMAIN") &&
                    !udfContext.getParameters().get("option").toString().equals("FIRST_SIGNIFICANT_SUBDOMAIN") &&
                    !udfContext.getParameters().get("option").toString().equals("FQDN")){
                throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "parameters option value is not correct");
            }
        }
        this.option = udfContext.getParameters().get("option").toString();
        this.lookupFields = udfContext.getLookup_fields();
        this.outputFieldName = udfContext.getOutput_fields().get(0);

    }


    @Override
    public Event evaluate(Event event) {
        String domain = "";
        for (String lookupField : lookupFields){

            if(event.getExtractedFields().containsKey(lookupField)){

                switch (option) {
                    case "TOP_LEVEL_DOMAIN":
                        domain =
                                FormatUtils.getTopPrivateDomain(
                                        (String)
                                                event.getExtractedFields()
                                                        .get(lookupField));
                        if(domain.contains(".")) {
                            domain = domain.substring(domain.indexOf(".") + 1);
                        }
                        break;
                    case "FIRST_SIGNIFICANT_SUBDOMAIN":
                        domain =
                                FormatUtils.getTopPrivateDomain(
                                        (String)
                                                event.getExtractedFields()
                                                        .get(lookupField));
                        break;
                    case "FQDN":
                            domain = (String) event.getExtractedFields()
                                    .get(lookupField);
                            break;

                }
            }
        }
        event.getExtractedFields().put(outputFieldName, domain);
        return event;
    }

    @Override
    public String functionName() {
        return "DOMAIN";
    }



    @Override
    public void close() {

    }
}
