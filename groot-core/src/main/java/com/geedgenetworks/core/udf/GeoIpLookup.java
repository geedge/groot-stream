package com.geedgenetworks.core.udf;

import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.config.EngineConfig;
import com.geedgenetworks.common.config.KnowledgeConfig;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;
import com.geedgenetworks.core.utils.KnowlegdeBase.IpKnowledgeBase;
import com.geedgenetworks.core.utils.KnowlegdeBase.KnowledgeBaseSchedule;
import com.geedgenetworks.utils.StringUtil;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

import java.util.HashMap;

public class GeoIpLookup implements UDF {

    private UDFContext udfContext;
    private static final Log logger = LogFactory.get();
    private String vender;
    private String option;

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {
        checkUdfContext(udfContext);
        this.udfContext = udfContext;
        this.vender = udfContext.getParameters().get("vendor_id").toString();
        this.option = udfContext.getParameters().get("option").toString();
        Configuration configuration = (Configuration) runtimeContext
                .getExecutionConfig().getGlobalJobParameters();
        EngineConfig engineConfig = com.alibaba.fastjson.JSON.parseObject(configuration.toMap().get(Constants.SYSPROP_GROOTSTREAM_CONFIG), EngineConfig.class);
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
        if (event.getExtractedFields().containsKey(udfContext.getLookup_fields().get(0))) {

            switch (option) {
                case "IP_TO_COUNTRY":
                    event.getExtractedFields()
                            .put(
                                    udfContext.getOutput_fields().get(0),
                                    IpKnowledgeBase.getVenderWithIpLookup()
                                            .get(vender)
                                            .countryLookup(
                                                    event.getExtractedFields()
                                                            .get(
                                                                    udfContext
                                                                            .getLookup_fields()
                                                                            .get(0))
                                                            .toString()));
                    break;
                case "IP_TO_PROVINCE":
                    event.getExtractedFields()
                            .put(
                                    udfContext.getOutput_fields().get(0),
                                    IpKnowledgeBase.getVenderWithIpLookup()
                                            .get(vender)
                                            .provinceLookup(
                                                    event.getExtractedFields()
                                                            .get(
                                                                    udfContext
                                                                            .getLookup_fields()
                                                                            .get(0))
                                                            .toString()));
                    break;
                case "IP_TO_CITY":
                    event.getExtractedFields()
                            .put(
                                    udfContext.getOutput_fields().get(0),
                                    IpKnowledgeBase.getVenderWithIpLookup()
                                            .get(vender)
                                            .cityLookup(
                                                    event.getExtractedFields()
                                                            .get(
                                                                    udfContext
                                                                            .getLookup_fields()
                                                                            .get(0))
                                                            .toString()));
                    break;
                case "IP_TO_SUBDIVISION_ADDR":
                    event.getExtractedFields()
                            .put(
                                    udfContext.getOutput_fields().get(0),
                                    IpKnowledgeBase.getVenderWithIpLookup()
                                            .get(vender)
                                            .cityLookupDetail(
                                                    event.getExtractedFields()
                                                            .get(
                                                                    udfContext
                                                                            .getLookup_fields()
                                                                            .get(0))
                                                            .toString()));
                    break;
                case "IP_TO_DETAIL":
                    event.getExtractedFields()
                            .put(
                                    udfContext.getOutput_fields().get(0),
                                    IpKnowledgeBase.getVenderWithIpLookup()
                                            .get(vender)
                                            .locationLookupDetail(
                                                    event.getExtractedFields()
                                                            .get(
                                                                    udfContext
                                                                            .getLookup_fields()
                                                                            .get(0))
                                                            .toString()));
                    break;
                case "IP_TO_LATLNG":
                    String geo =
                            IpKnowledgeBase.getVenderWithIpLookup()
                                    .get(vender)
                                    .latLngLookup(
                                            event.getExtractedFields()
                                                    .get(
                                                            udfContext
                                                                    .getLookup_fields()
                                                                    .get(0))
                                                    .toString());
                    String[] latLng = geo.split(",");
                    if (latLng.length == 2) {
                        event.getExtractedFields()
                                .put(udfContext.getOutput_fields().get(0), latLng[0]);
                        event.getExtractedFields()
                                .put(udfContext.getOutput_fields().get(1), latLng[1]);
                    }
                    break;
                case "IP_TO_PROVIDER":
                    HashMap<String, Object> serverIpMap =
                            JSON.parseObject(
                                    IpKnowledgeBase.getVenderWithIpLookup()
                                            .get(vender)
                                            .ispLookup(
                                                    event.getExtractedFields()
                                                            .get(
                                                                    udfContext
                                                                            .getLookup_fields()
                                                                            .get(0))
                                                            .toString()),
                                    TypeReference.mapType(
                                            HashMap.class, String.class, Object.class));
                    event.getExtractedFields()
                            .put(
                                    udfContext.getOutput_fields().get(0),
                                    serverIpMap.getOrDefault("isp", StringUtil.EMPTY));
                    break;
                case "IP_TO_JSON ":
                    event.getExtractedFields()
                            .put(
                                    udfContext.getOutput_fields().get(0),
                                    IpKnowledgeBase.getVenderWithIpLookup()
                                            .get(vender)
                                            .infoLookupToJSONString(
                                                    event.getExtractedFields()
                                                            .get(
                                                                    udfContext
                                                                            .getLookup_fields()
                                                                            .get(0))
                                                            .toString()));
                    break;
                case "IP_TO_OBJECT":
                    event.getExtractedFields()
                            .put(
                                    udfContext.getOutput_fields().get(0),
                                    IpKnowledgeBase.getVenderWithIpLookup()
                                            .get(vender)
                                            .infoLookup(
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
        }
        return event;
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
            if(!udfContext.getParameters().get("option").toString().equals("IP_TO_COUNTRY") && //IP_TO_COUNTRY
                    !udfContext.getParameters().get("option").toString().equals("IP_TO_PROVINCE") && //IP_TO_PROVINCE
                    !udfContext.getParameters().get("option").toString().equals("IP_TO_CITY") && //IP_TO_CITY
                    !udfContext.getParameters().get("option").toString().equals("IP_TO_SUBDIVISION_ADDR") && //IP_TO_SUBDIVISION_ADDR
                    !udfContext.getParameters().get("option").toString().equals("IP_TO_DETAIL") && //IP_TO_DETAIL
                    !udfContext.getParameters().get("option").toString().equals("IP_TO_LATLNG") && //IP_TO_LATLNG
                    !udfContext.getParameters().get("option").toString().equals("IP_TO_PROVIDER") && //IP_TO_PROVIDER
                    !udfContext.getParameters().get("option").toString().equals("IP_TO_JSON") && //IP_TO_JSON
                    !udfContext.getParameters().get("option").toString().equals("IP_TO_OBJECT") ){
                throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "parameters option value is not correct");
            }
            if(udfContext.getParameters().get("option").toString().equals("IP_TO_OBJECT")){

                if(!udfContext.getParameters().containsKey("geolocation_field_mapping")){

                    throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "parameters need containkey geolocation_field_mapping");

                }

            }
        }

    }


    @Override
    public String functionName() {
        return "IP_LOCATION";
    }

    @Override
    public void close() {

    }
}
