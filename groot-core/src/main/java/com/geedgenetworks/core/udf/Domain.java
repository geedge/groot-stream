package com.geedgenetworks.core.udf;

import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.udf.ScalarFunction;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.udf.UDFContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.util.List;

import static com.geedgenetworks.utils.FormatUtils.getTopPrivateDomain;

@Slf4j
public class Domain implements ScalarFunction {


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
        switch (option) {
            case "TOP_LEVEL_DOMAIN":
                for (String lookupField : lookupFields){

                    Object valueObj = event.getExtractedFields().get(lookupField);
                    if (valueObj!=null) {
                        domain = getTopPrivateDomain((String) valueObj);
                        if (domain.contains(".")) {
                            domain = domain.substring(domain.indexOf(".") + 1);
                        }
                        if(!domain.isEmpty()){
                            break;
                        }
                    }
                }
                break;
            case "FIRST_SIGNIFICANT_SUBDOMAIN":
                for (String lookupField : lookupFields){
                    Object valueObj = event.getExtractedFields().get(lookupField);
                    if (valueObj!=null) {
                        domain = getTopPrivateDomain((String)valueObj);
                    }
                    if(!domain.isEmpty()){
                        break;
                    }
                }
                break;
            case "FQDN":
                for (String lookupField : lookupFields) {
                    Object valueObj = event.getExtractedFields().get(lookupField);
                    if (valueObj!=null) {
                        domain = (String)valueObj;
                    }
                    if(!domain.isEmpty()){
                        break;
                    }
                }
                break;
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
