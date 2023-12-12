package com.geedgenetworks.core.udf;

import com.geedgenetworks.common.Constants;
import com.geedgenetworks.common.config.EngineConfig;
import com.geedgenetworks.common.config.KnowledgeConfig;
import com.geedgenetworks.common.exception.CommonErrorCode;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;
import com.geedgenetworks.core.utils.KnowlegdeBase.IpKnowledgeBase;
import com.geedgenetworks.core.utils.KnowlegdeBase.KnowledgeBaseUpdateJob;
import com.geedgenetworks.domain.LocationResponse;
import com.geedgenetworks.utils.StringUtil;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.quartz.SchedulerException;

import java.util.HashMap;
import java.util.Map;

import static com.geedgenetworks.core.utils.SchedulerUtils.shutdownScheduler;

@Slf4j
public class GeoIpLookup implements UDF {

    private UDFContext udfContext;
    private String vender;
    private String option;

    private Map<String,String> geolocation_field_mapping;

    @Override
    public void open(RuntimeContext runtimeContext, UDFContext udfContext) {
        checkUdfContext(udfContext);
        this.udfContext = udfContext;
        this.vender = udfContext.getParameters().get("vendor_id").toString();
        this.option = udfContext.getParameters().get("option").toString();
        if(option.equals("IP_TO_OBJECT")){
            this.geolocation_field_mapping = (Map<String, String>) udfContext.getParameters().get("geolocation_field_mapping");
        }
        KnowledgeBaseUpdateJob.initKnowledgeBase(vender,IpKnowledgeBase.getInstance(),runtimeContext);

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

                        event.getExtractedFields()
                                .put(udfContext.getOutput_fields().get(0), geo);

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

                    LocationResponse response = (LocationResponse) IpKnowledgeBase.getVenderWithIpLookup()
                            .get(vender)
                            .infoLookup(
                                    event.getExtractedFields()
                                            .get(
                                                    udfContext
                                                            .getLookup_fields()
                                                            .get(0))
                                            .toString());

                    for (Map.Entry<String, String> entry : geolocation_field_mapping.entrySet()) {
                        switch (entry.getKey()) {
                            case "COUNTRY":
                                event.getExtractedFields()
                                        .put(entry.getValue(), response.getCountry());
                                break;
                            case "PROVINCE":
                                event.getExtractedFields()
                                        .put(entry.getValue(), response.getSuperAdministrativeArea());
                                break;
                            case "CITY":
                                event.getExtractedFields()
                                        .put(entry.getValue(), response.getAdministrativeArea());
                                break;
                            case "LONGITUDE":
                                event.getExtractedFields()
                                        .put(entry.getValue(), response.getLongitude());
                                break;
                            case "LATITUDE":
                                event.getExtractedFields()
                                        .put(entry.getValue(), response.getLatitude());
                                break;
                            case "ISP":
                                event.getExtractedFields()
                                        .put(entry.getValue(), response.getIsp());
                                break;
                            case "ORGANIZATION ":
                                event.getExtractedFields()
                                        .put(entry.getValue(), response.getOrganization());
                                break;
                            default:
                                break;
                        }
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
                    }
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
                else {
                    Map<String,String> geolocation_field_mapping =(Map<String, String>) udfContext.getParameters().get("geolocation_field_mapping");

                    if(!geolocation_field_mapping.isEmpty()){

                        for (Map.Entry<String, String> entry : geolocation_field_mapping.entrySet()) {

                            if(!entry.getKey().equals("COUNTRY") && !entry.getKey().equals("PROVINCE") && !entry.getKey().equals("CITY") && !entry.getKey().equals("LONGITUDE") && !entry.getKey().equals("LATITUDE") && !entry.getKey().equals("ISP") && !entry.getKey().equals("ORGANIZATION")){
                                throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "parameters geolocation_field_mapping value is not correct");
                            }
                        }
                    }
                    else {
                        throw new GrootStreamRuntimeException(CommonErrorCode.ILLEGAL_ARGUMENT, "parameters geolocation_field_mapping value is not correct");
                    }
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
        try {
            shutdownScheduler();
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }
}
