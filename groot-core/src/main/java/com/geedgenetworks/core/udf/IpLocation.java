package com.geedgenetworks.core.udf;

import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;
import com.geedgenetworks.core.utils.IPUtils;
import com.geedgenetworks.utils.StringUtil;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;

import java.util.HashMap;

public class IpLocation implements UDF {

    private UDFContext udfContext;
    private static final Log logger = LogFactory.get();
    private String vender;
    private String option;

    @Override
    public void init(UDFContext udfContext) {
        this.udfContext = udfContext;
        this.vender = udfContext.getParameters().get("kb_vender").toString();
        this.option = udfContext.getParameters().get("option").toString();
    }

    @Override
    public Event evaluate(Event event) {
        if (event.getExtractedFields().containsKey(udfContext.getLookup_fields().get(0))) {

            switch (option) {
                case "IP_TO_COUNTRY":
                    event.getExtractedFields()
                            .put(
                                    udfContext.getOutput_fields().get(0),
                                    IPUtils.venderWithIpLookup
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
                                    IPUtils.venderWithIpLookup
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
                                    IPUtils.venderWithIpLookup
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
                                    IPUtils.venderWithIpLookup
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
                                    IPUtils.venderWithIpLookup
                                            .get(vender)
                                            .locationLookupDetail(
                                                    event.getExtractedFields()
                                                            .get(
                                                                    udfContext
                                                                            .getLookup_fields()
                                                                            .get(0))
                                                            .toString()));
                    break;
                case "IP_TO_GEO":
                    String geo =
                            IPUtils.venderWithIpLookup
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
                                    IPUtils.venderWithIpLookup
                                            .get(vender)
                                            .asnLookupDetail(
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
                default:
                    break;
            }
        }
        return event;
    }

    @Override
    public String functionName() {
        return "IP_LOCATION";
    }

    @Override
    public void close() {

    }
}
