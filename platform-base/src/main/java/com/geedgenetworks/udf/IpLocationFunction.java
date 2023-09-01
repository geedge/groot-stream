package com.geedgenetworks.udf;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.pojo.ProjectionFunction;
import com.geedgenetworks.utils.IPUtils;
import com.zdjizhi.utils.StringUtil;

import java.util.HashMap;

public class IpLocationFunction implements UdfFunction {

    private ProjectionFunction projection_function;
    private static final Log logger = LogFactory.get();

    private final String id ;
    private final String option ;


    public IpLocationFunction(ProjectionFunction projection_function) {
        this.projection_function=projection_function;
        this.id =  projection_function.getParameters().get("kb_id").toString();
        this.option =  projection_function.getParameters().get("option").toString();

    }


    @Override
    public void udffunction(Event event) {

        if(StringUtil.isNotEmpty(id)) {

            switch (option) {
                case "IP_TO_COUNTRY" :
                    event.getExtractedFields().put(projection_function.getOutput_fields().get(0),IPUtils.ipLookupV2Map.get(id).countryLookup( event.getExtractedFields().get(projection_function.getLookup_fields().get(0)).toString()));
                    System.out.println(event.getExtractedFields().get(projection_function.getLookup_fields().get(0)).toString() +IPUtils.ipLookupV2Map.get(id).countryLookup( event.getExtractedFields().get(projection_function.getLookup_fields().get(0)).toString()) );

                    break;
                case "IP_TO_PROVINCE":
                    event.getExtractedFields().put(projection_function.getOutput_fields().get(0),IPUtils.ipLookupV2Map.get(id).provinceLookup( event.getExtractedFields().get(projection_function.getLookup_fields().get(0)).toString()));
                    System.out.println(event.getExtractedFields().get(projection_function.getLookup_fields().get(0)).toString() +IPUtils.ipLookupV2Map.get(id).provinceLookup( event.getExtractedFields().get(projection_function.getLookup_fields().get(0)).toString()) );

                    break;
                case "IP_TO_CITY":
                    event.getExtractedFields().put(projection_function.getOutput_fields().get(0),IPUtils.ipLookupV2Map.get(id).cityLookup( event.getExtractedFields().get(projection_function.getLookup_fields().get(0)).toString()));
                    System.out.println(event.getExtractedFields().get(projection_function.getLookup_fields().get(0)).toString() +IPUtils.ipLookupV2Map.get(id).cityLookup( event.getExtractedFields().get(projection_function.getLookup_fields().get(0)).toString()) );

                    break;

                case "IP_TO_SUBDIVISION_ADDR":
                    event.getExtractedFields().put(projection_function.getOutput_fields().get(0),IPUtils.ipLookupV2Map.get(id).cityLookupDetail( event.getExtractedFields().get(projection_function.getLookup_fields().get(0)).toString()));
                    System.out.println(event.getExtractedFields().get(projection_function.getLookup_fields().get(0)).toString() +IPUtils.ipLookupV2Map.get(id).cityLookupDetail( event.getExtractedFields().get(projection_function.getLookup_fields().get(0)).toString()) );

                    break;

                case "IP_TO_DETAIL":
                    event.getExtractedFields().put(projection_function.getOutput_fields().get(0),IPUtils.ipLookupV2Map.get(id).locationLookupDetail( event.getExtractedFields().get(projection_function.getLookup_fields().get(0)).toString()));
                    System.out.println(event.getExtractedFields().get(projection_function.getLookup_fields().get(0)).toString() +IPUtils.ipLookupV2Map.get(id).locationLookupDetail( event.getExtractedFields().get(projection_function.getLookup_fields().get(0)).toString()) );

                    break;

                case "IP_TO_GEO":
                        String geo = IPUtils.ipLookupV2Map.get(id).latLngLookup(event.getExtractedFields().get(projection_function.getLookup_fields().get(0)).toString());
                        String[] latLng = geo.split(",");
                        if (latLng.length == 2) {

                            event.getExtractedFields().put(projection_function.getOutput_fields().get(0), latLng[0]);
                            event.getExtractedFields().put(projection_function.getOutput_fields().get(1), latLng[1]);

                        }
                    System.out.println(event.getExtractedFields().get(projection_function.getLookup_fields().get(0)).toString() +IPUtils.ipLookupV2Map.get(id).latLngLookup( event.getExtractedFields().get(projection_function.getLookup_fields().get(0)).toString()) );

                    break;

                case "IP_TO_PROVIDER":

                    HashMap<String, Object> serverIpMap = JSON.parseObject(IPUtils.ipLookupV2Map.get(id).infoLookupToJson(event.getExtractedFields().get(projection_function.getLookup_fields().get(0)).toString()), TypeReference.mapType(HashMap.class, String.class, Object.class));
                    System.out.println("isp:" + serverIpMap.getOrDefault("isp", StringUtil.EMPTY));
                    event.getExtractedFields().put(projection_function.getOutput_fields().get(0),serverIpMap.getOrDefault("isp", StringUtil.EMPTY));
                    break;
                default:
                    break;
            }

        }
        else {
            logger.error("知识库id为空");

        }

    }
}
