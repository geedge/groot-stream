package com.geedgenetworks.core.udf.test;

import com.geedgenetworks.common.config.KnowledgeConfig;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.core.pojo.UDFContext;
import com.geedgenetworks.core.udf.GeoIpLookup;
import com.geedgenetworks.core.utils.KnowlegdeBase.GeoIpKnowledgeBase;
import com.geedgenetworks.domain.LocationResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class GeoIpLookupFunctionTest {

    private static UDFContext udfContext;

    private static Map<String, Object> parameters ;

    @BeforeAll
    public static void setUp() {
        udfContext = new UDFContext();
        parameters = new HashMap<>();
        udfContext.setParameters(parameters);
        udfContext.setLookup_fields(Arrays.asList("ip"));
        udfContext.setOutput_fields(Arrays.asList("iplocation"));
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

       /* udfContext.setParameters(new HashMap<>());
        udfContext.getParameters().put("option","IP_TO_ASN");
        udfContext.getParameters().put("vendor_id","tsg_asnlookup");*/

        KnowledgeConfig knowledgeConfig =new KnowledgeConfig();
        knowledgeConfig.setName("tsg_geoiplookup");
        knowledgeConfig.setType("geoiplookup");
        knowledgeConfig.setFiles(Arrays.asList("acf1db8589c5e277-ead1a65e1c3973dc","acf1db8589c5e277-ead1a65e1c3973dc"));
        GeoIpKnowledgeBase geoIpKnowledgeBase = GeoIpKnowledgeBase.getInstance();
        geoIpKnowledgeBase.updateKnowledgeBase(knowledgeConfig);
        String countryLookup  = GeoIpKnowledgeBase.getVenderWithIpLookup().get("tsg_geoiplookup").countryLookup("2600:1015:b002::");
        String provinceLookup  = GeoIpKnowledgeBase.getVenderWithIpLookup().get("tsg_geoiplookup").provinceLookup("2600:1015:b002::");
        String cityLookup  = GeoIpKnowledgeBase.getVenderWithIpLookup().get("tsg_geoiplookup").cityLookup("2600:1015:b002::");
        String cityLookupDetail  = GeoIpKnowledgeBase.getVenderWithIpLookup().get("tsg_geoiplookup").cityLookupDetail("2600:1015:b002::");
        String locationLookupDetail  = GeoIpKnowledgeBase.getVenderWithIpLookup().get("tsg_geoiplookup").locationLookupDetail("2600:1015:b002::");
        String latLngLookup  = GeoIpKnowledgeBase.getVenderWithIpLookup().get("tsg_geoiplookup").latLngLookup("2600:1015:b002::");
        String ispLookup  = GeoIpKnowledgeBase.getVenderWithIpLookup().get("tsg_geoiplookup").ispLookup("2600:1015:b002::");
        String infoLookupToJSONString  = GeoIpKnowledgeBase.getVenderWithIpLookup().get("tsg_geoiplookup").infoLookupToJSONString("2600:1015:b002::");
        LocationResponse infoLookup  = (LocationResponse) GeoIpKnowledgeBase.getVenderWithIpLookup().get("tsg_geoiplookup").infoLookup("2600:1015:b002::");


        System.out.println("countryLookup:" + countryLookup);
         System.out.println("provinceLookup:" + provinceLookup);
         System.out.println("cityLookup:" + cityLookup);
         System.out.println("cityLookupDetail:" + cityLookupDetail);
         System.out.println("locationLookupDetail:" + locationLookupDetail);
         System.out.println("latLngLookup:" + latLngLookup);
         System.out.println("ispLookup:" + ispLookup);
         System.out.println("infoLookupToJSONString:" + infoLookupToJSONString);
         System.out.println("infoLookup:" + GeoIpKnowledgeBase.getVenderWithIpLookup().get("tsg_geoiplookup").infoLookup("2600:1015:b002::"));
         System.out.println(infoLookup+"----------------------------");


       // assertEquals("6167", asn);

    }


}
