package com.geedgenetworks.core.udf;

import com.geedgenetworks.common.config.Constants;
import com.geedgenetworks.common.config.*;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.core.udf.knowlegdebase.handler.GeoIpKnowledgeBaseHandler;
import com.geedgenetworks.core.udf.knowlegdebase.KnowledgeBaseUpdateJob;

import com.alibaba.fastjson2.JSON;
import com.geedgenetworks.model.LocationResponse;
import com.geedgenetworks.api.common.udf.ScalarFunction;
import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.configuration.CheckUDFContextUtil;
import com.geedgenetworks.api.configuration.UDFContextConfigOptions;
import com.geedgenetworks.api.connector.event.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

import java.util.Map;

@Slf4j
public class GeoIpLookup implements ScalarFunction {

    private String kbName;
    private String option;
    private String lookupFieldName;
    private String outputFieldName;
    private Map<String, String> geoLocationFieldMapping;

    enum Option {
        IP_TO_COUNTRY,
        IP_TO_PROVINCE,
        IP_TO_CITY,
        IP_TO_SUBDIVISION_ADDR,
        IP_TO_DETAIL,
        IP_TO_LATLNG,
        IP_TO_PROVIDER,
        IP_TO_JSON,
        IP_TO_OBJECT
        ;

        public static boolean isValid(String option) {
            try {
                Option.valueOf(option);
                return true;
            } catch (IllegalArgumentException e) {
                return false;
            }
        }
    }

    enum GeolocationFieldMapping {
        COUNTRY,
        PROVINCE,
        CITY,
        LONGITUDE,
        LATITUDE,
        ISP,
        ORGANIZATION
        ;

        public static boolean isValid(String option) {
            try {
                GeolocationFieldMapping.valueOf(option);
                return true;
            } catch (IllegalArgumentException e) {
                return false;
            }
        }
    }

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {
        checkConfig(udfContext);
        this.kbName = udfContext.getParameters().get(UDFContextConfigOptions.PARAMETERS_KB_NAME.key()).toString();
        this.option = udfContext.getParameters().get(UDFContextConfigOptions.PARAMETERS_OPTION.key()).toString();
        if (option.equals(Option.IP_TO_OBJECT.name())) {
            this.geoLocationFieldMapping = (Map<String, String>) udfContext.getParameters().get(UDFContextConfigOptions.PARAMETERS_GEOLOCATION_FIELD_MAPPING.key());
        }
        Configuration configuration = (Configuration) runtimeContext
                .getExecutionConfig().getGlobalJobParameters();
        CommonConfig commonConfig = JSON.parseObject(configuration.toMap().get(Constants.SYSPROP_GROOTSTREAM_CONFIG), CommonConfig.class);
        for (KnowledgeBaseConfig knowledgeBaseConfig : commonConfig.getKnowledgeBaseConfig()) {
            if (kbName.equals(knowledgeBaseConfig.getName())) {
                KnowledgeBaseUpdateJob.registerKnowledgeBase(GeoIpKnowledgeBaseHandler.getInstance(), knowledgeBaseConfig);
                break;
            }
        }
        if (GeoIpKnowledgeBaseHandler.getKbNameWithKbConfigAndFileMetas().containsKey(kbName) && GeoIpKnowledgeBaseHandler.getKbNameWithKbConfigAndFileMetas().get(kbName).getIpLookupV2() != null) {
            log.debug("Init function GeoIpLookup success ");
        } else {
            log.error("GeoIpLookup init KnowledgeBase error ");
        }
        this.lookupFieldName = udfContext.getLookupFields().get(0);
        if(udfContext.getOutputFields()!=null && !udfContext.getOutputFields().isEmpty()){
            this.outputFieldName = udfContext.getOutputFields().get(0);
        }
        GeoIpKnowledgeBaseHandler.increment();
        log.warn("GeoIpKnowledgeBaseHandler "+GeoIpKnowledgeBaseHandler.getCount());
    }


    @Override
    public Event evaluate(Event event) {
        Object valueObj = event.getExtractedFields().get(lookupFieldName);
        if (valueObj!=null) {
            if (Option.IP_TO_OBJECT.name().equals(option)) {
                LocationResponse response  = GeoIpKnowledgeBaseHandler.lookUpObject(kbName,valueObj.toString());
                for (Map.Entry<String, String> entry : geoLocationFieldMapping.entrySet()) {
                    String result = "";
                    if (response!=null) {
                        switch (entry.getKey()) {
                            case "COUNTRY":
                                result = response.getCountry();
                                break;
                            case "PROVINCE":
                                result = response.getSuperAdministrativeArea();
                                break;
                            case "CITY":
                                result = response.getAdministrativeArea();
                                break;
                            case "LONGITUDE":
                                result = response.getLongitude();
                                break;
                            case "LATITUDE":
                                result = response.getLatitude();
                                break;
                            case "ISP":
                                result = response.getIsp();
                                break;
                            case "ORGANIZATION":
                                result = response.getOrganization();
                                break;
                            default:
                                break;
                        }
                        event.getExtractedFields().put(entry.getValue(), result);
                    }
                }
            } else {
                event.getExtractedFields().put(outputFieldName, GeoIpKnowledgeBaseHandler.lookUp(kbName, option, valueObj.toString()));
            }
        }
        return event;
    }

    @Override
    public void checkConfig(UDFContext udfContext) {
        CheckResult result = CheckUDFContextUtil.checkAllExists(udfContext,
                UDFContextConfigOptions.LOOKUP_FIELDS.key(),
                UDFContextConfigOptions.PARAMETERS.key());

        if (!result.isSuccess()) {
            throw new GrootStreamRuntimeException(CommonErrorCode.CONFIG_VALIDATION_FAILED, result.getMsg());
        }

        result = CheckUDFContextUtil.checkCollectionSingleItemExists(udfContext, UDFContextConfigOptions.LOOKUP_FIELDS.key());
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

        if (optionValue.equals(Option.IP_TO_OBJECT.name())) {
            result = CheckUDFContextUtil.checkParametersContainsKeys(udfContext, UDFContextConfigOptions.PARAMETERS_GEOLOCATION_FIELD_MAPPING.key());
            if (!result.isSuccess()) {
                throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, result.getMsg());
            }

           Map<String, String> fieldMap =  (Map<String, String>) udfContext.getParameters().get(UDFContextConfigOptions.PARAMETERS_GEOLOCATION_FIELD_MAPPING.key());

            for (Map.Entry<String, String> entry : fieldMap.entrySet()) {
                if (!GeolocationFieldMapping.isValid(entry.getKey())) {
                    throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "parameters geolocation_field_mapping value is not correct");
                }
            }

        }

    }

    @Override
    public String functionName() {
        return "GEOIP_LOOKUP";
    }

    @Override
    public void close() {
        GeoIpKnowledgeBaseHandler.decrement();
        if (GeoIpKnowledgeBaseHandler.getCount()<=0) {
            KnowledgeBaseUpdateJob.removeKnowledgeBase(kbName);
            log.warn("GeoIpKnowledgeBaseHandler removeKnowledgeBase "+kbName);
        }
    }
}
