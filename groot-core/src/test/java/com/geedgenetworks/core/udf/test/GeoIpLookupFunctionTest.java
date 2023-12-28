package com.geedgenetworks.core.udf.test;

import com.geedgenetworks.common.config.CommonConfig;
import com.geedgenetworks.common.config.KnowledgeBaseConfig;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.core.pojo.UDFContext;
import com.geedgenetworks.core.udf.GeoIpLookup;
import com.geedgenetworks.core.utils.KnowlegdeBase.AsnKnowledgeBase;
import com.geedgenetworks.core.utils.KnowlegdeBase.GeoIpKnowledgeBase;
import com.geedgenetworks.core.utils.KnowlegdeBase.KnowledgeBaseUpdateJob;
import com.geedgenetworks.domain.LocationResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class GeoIpLookupFunctionTest {

    private static UDFContext udfContext;


    @BeforeAll
    public static void setUp() {
        udfContext = new UDFContext();
        udfContext.setLookup_fields(Collections.singletonList("ip"));
        udfContext.setOutput_fields(Collections.singletonList("iplocation"));
    }

//com.geedgenetworks.core.udf.GeoIpLookup单元测试
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
    public void testAsnLookupFunctionIpToAsn() throws Exception {

        GeoIpLookup geoIpLookup = new GeoIpLookup();

        CommonConfig commonConfig = new CommonConfig();
        List<KnowledgeBaseConfig> knowledgeBaseConfigList = new ArrayList<>();
        KnowledgeBaseConfig knowledgeBaseConfig = new KnowledgeBaseConfig();
        knowledgeBaseConfig.setName("tsg_geoiplookup");
        Map<String, String> parameters = new HashMap<>();
        parameters.put("fs_type","hos");
        parameters.put("fs_default_path","http://192.168.44.12:9999/v1/knowledge_base?kb_id=");
        knowledgeBaseConfig.setProperties(parameters);
        knowledgeBaseConfig.setType("geoiplookup");
        knowledgeBaseConfig.setFiles(Arrays.asList("64af7077-eb9b-4b8f-80cf-2ceebc89bea9","004390bc-3135-4a6f-a492-3662ecb9e289"));
        knowledgeBaseConfigList.add(knowledgeBaseConfig);
        commonConfig.setKnowledgeBaseConfig(knowledgeBaseConfigList);
        KnowledgeBaseUpdateJob.initKnowledgeBase("tsg_geoiplookup", GeoIpKnowledgeBase.getInstance(), commonConfig);
        String countryLookup  = GeoIpKnowledgeBase.getVenderWithIpLookup().get("tsg_geoiplookup").countryLookup("2600:1015:b002::");
        assertEquals("美国", countryLookup);
        String provinceLookup  = GeoIpKnowledgeBase.getVenderWithIpLookup().get("tsg_geoiplookup").provinceLookup("2600:1015:b002::");
        assertEquals("印第安纳州", provinceLookup);
        String cityLookup  = GeoIpKnowledgeBase.getVenderWithIpLookup().get("tsg_geoiplookup").cityLookup("2600:1015:b002::");
        assertEquals("Unknown", cityLookup);
        String cityLookupDetail  = GeoIpKnowledgeBase.getVenderWithIpLookup().get("tsg_geoiplookup").cityLookupDetail("2600:1015:b002::");
        assertEquals("美国.印第安纳州.Unknown", cityLookupDetail);
        String locationLookupDetail  = GeoIpKnowledgeBase.getVenderWithIpLookup().get("tsg_geoiplookup").locationLookupDetail("2600:1015:b002::");
        assertEquals("美国.印第安纳州.Unknown. . . .", locationLookupDetail.trim());
        String latLngLookup  = GeoIpKnowledgeBase.getVenderWithIpLookup().get("tsg_geoiplookup").latLngLookup("2600:1015:b002::");
        assertEquals("39.9388,-86.1137", latLngLookup);
        String ispLookup  = GeoIpKnowledgeBase.getVenderWithIpLookup().get("tsg_geoiplookup").ispLookup("2600:1015:b002::");
        assertEquals("", ispLookup);
        String infoLookupToJSONString  = GeoIpKnowledgeBase.getVenderWithIpLookup().get("tsg_geoiplookup").infoLookupToJSONString("2600:1015:b002::");
        assertEquals("{\"administrativeArea\":\"Unknown\",\"areaCode\":\" \",\"asn\":\" \",\"country\":\"美国\",\"countryCode\":\"\",\"dependentLocality\":\" \",\"doubleDependentLocality\":\" \",\"isp\":\" \",\"latitude\":\"39.9388\",\"locality\":\" \",\"longitude\":\"-86.1137\",\"organization\":\"\",\"privateIP\":false,\"subAdministrativeArea\":\" \",\"superAdministrativeArea\":\"印第安纳州\"}", infoLookupToJSONString);
        LocationResponse infoLookup  = (LocationResponse) GeoIpKnowledgeBase.getVenderWithIpLookup().get("tsg_geoiplookup").infoLookup("2600:1015:b002::");
        assertEquals("美国", infoLookup.getCountry());

    }


}
