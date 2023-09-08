package com.geedgenetworks.udf;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import com.geedgenetworks.pojo.Event;
import com.geedgenetworks.pojo.ProjectionFunction;
import com.geedgenetworks.utils.IPUtils;
import com.geedgenetworks.utils.StringUtil;

import java.util.HashMap;

public class IpLocationFunction implements UdfFunction {

    private ProjectionFunction projection_function;
    private static final Log logger = LogFactory.get();

    private final String vender;
    private final String option;


    public IpLocationFunction(ProjectionFunction projection_function) {
        this.projection_function = projection_function;
        this.vender = projection_function.getParameters().get("kb_vender").toString();
        this.option = projection_function.getParameters().get("option").toString();

    }


    @Override
    public void udffunction(Event event) {

        if (event.getExtractedFields().containsKey(projection_function.getLookup_fields().get(0))) {

            switch (option) {
                case "IP_TO_COUNTRY":
                    event.getExtractedFields().put(projection_function.getOutput_fields().get(0), IPUtils.venderWithIpLookup.get(vender).countryLookup(event.getExtractedFields().get(projection_function.getLookup_fields().get(0)).toString()));

                    break;
                case "IP_TO_PROVINCE":
                    event.getExtractedFields().put(projection_function.getOutput_fields().get(0), IPUtils.venderWithIpLookup.get(vender).provinceLookup(event.getExtractedFields().get(projection_function.getLookup_fields().get(0)).toString()));

                    break;
                case "IP_TO_CITY":
                    event.getExtractedFields().put(projection_function.getOutput_fields().get(0), IPUtils.venderWithIpLookup.get(vender).cityLookup(event.getExtractedFields().get(projection_function.getLookup_fields().get(0)).toString()));

                    break;

                case "IP_TO_SUBDIVISION_ADDR":
                    event.getExtractedFields().put(projection_function.getOutput_fields().get(0), IPUtils.venderWithIpLookup.get(vender).cityLookupDetail(event.getExtractedFields().get(projection_function.getLookup_fields().get(0)).toString()));
                    break;
                case "IP_TO_DETAIL":
                    event.getExtractedFields().put(projection_function.getOutput_fields().get(0), IPUtils.venderWithIpLookup.get(vender).locationLookupDetail(event.getExtractedFields().get(projection_function.getLookup_fields().get(0)).toString()));
                    break;

                case "IP_TO_GEO":
                    String geo = IPUtils.venderWithIpLookup.get(vender).latLngLookup(event.getExtractedFields().get(projection_function.getLookup_fields().get(0)).toString());
                    String[] latLng = geo.split(",");
                    if (latLng.length == 2) {

                        event.getExtractedFields().put(projection_function.getOutput_fields().get(0), latLng[0]);
                        event.getExtractedFields().put(projection_function.getOutput_fields().get(1), latLng[1]);

                    }
                    break;

                case "IP_TO_PROVIDER":

                    HashMap<String, Object> serverIpMap = JSON.parseObject(IPUtils.venderWithIpLookup.get(vender).asnLookupDetail(event.getExtractedFields().get(projection_function.getLookup_fields().get(0)).toString()), TypeReference.mapType(HashMap.class, String.class, Object.class));
                    event.getExtractedFields().put(projection_function.getOutput_fields().get(0), serverIpMap.getOrDefault("isp", StringUtil.EMPTY));
                    break;
                default:
                    break;
            }
        }

    }
}
