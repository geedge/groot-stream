package com.geedgenetworks.core.udf.test;

import com.alibaba.fastjson2.JSONObject;
import com.geedgenetworks.common.config.CommonConfig;
import com.geedgenetworks.common.config.KnowledgeBaseConfig;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.udf.UDFContext;
import com.geedgenetworks.core.udf.GeoIpLookup;
import com.geedgenetworks.core.udf.knowlegdebase.handler.GeoIpKnowledgeBaseHandler;
import com.geedgenetworks.core.udf.knowlegdebase.KnowledgeBaseUpdateJob;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class GeoIpLookupFunctionTest {

    private static UDFContext udfContext;


    @BeforeAll
    public static void setUp() {
        udfContext = new UDFContext();
        udfContext.setLookup_fields(Collections.singletonList("ip"));
        udfContext.setOutput_fields(Collections.singletonList("iplocation"));
    }

    @Test
    public void testInit(){
        GeoIpLookup geoIpLookup = new GeoIpLookup();
        udfContext.setLookup_fields(new ArrayList<>());
        udfContext.setParameters(new HashMap<>());
        udfContext.setParameters(null);
        Assertions.assertThrows(GrootStreamRuntimeException.class, () -> {
            geoIpLookup.open(null, udfContext);
        });
        udfContext.setParameters(new HashMap<>());
        udfContext.getParameters().put("option","IP_TO_CITY");
        Assertions.assertThrows(GrootStreamRuntimeException.class, () -> {
            geoIpLookup.open(null, udfContext);
        });

        udfContext.setLookup_fields(new ArrayList<>());
        udfContext.getLookup_fields().add("v1");
        udfContext.setOutput_fields(new ArrayList<>());
        udfContext.getOutput_fields().add("v2");
        udfContext.setParameters(new HashMap<>());
        udfContext.getParameters().put("option","other");
        Assertions.assertThrows(GrootStreamRuntimeException.class, () -> {
            geoIpLookup.open(null, udfContext);
        });

    }



    @Test
    public void testGeoIpLookupFunction() throws Exception {
        CommonConfig commonConfig = new CommonConfig();
        List<KnowledgeBaseConfig> knowledgeBaseConfigList = new ArrayList<>();
        KnowledgeBaseConfig knowledgeBaseConfig = new KnowledgeBaseConfig();
        knowledgeBaseConfig.setName("tsg_ip_location");
        Map<String, String> parameters = new HashMap<>();
        knowledgeBaseConfig.setProperties(parameters);
        knowledgeBaseConfig.setFsType("local");
        knowledgeBaseConfig.setFsPath("");
        knowledgeBaseConfig.setFiles(Collections.singletonList("src/test/resources/ip_builtin.mmdb"));
        knowledgeBaseConfigList.add(knowledgeBaseConfig);
        commonConfig.setKnowledgeBaseConfig(knowledgeBaseConfigList);
        KnowledgeBaseUpdateJob.registerKnowledgeBase(GeoIpKnowledgeBaseHandler.getInstance(), knowledgeBaseConfig);
        String countryLookup  = GeoIpKnowledgeBaseHandler.lookUp("tsg_ip_location","IP_TO_COUNTRY","86.111.175.128");
        assertEquals("United Kingdom", countryLookup);
        String country = GeoIpKnowledgeBaseHandler.lookUpObject("tsg_ip_location","86.111.175.128").getCountry();
        assertEquals("United Kingdom", country);
        assertNull(GeoIpKnowledgeBaseHandler.lookUpObject("tsg_ip_location", "255.255.255.255"));

    }
}
