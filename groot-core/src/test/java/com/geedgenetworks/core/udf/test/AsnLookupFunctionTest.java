package com.geedgenetworks.core.udf.test;

import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.core.pojo.UDFContext;
import com.geedgenetworks.core.udf.AsnLookup;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AsnLookupFunctionTest {

    private static UDFContext udfContext;

    private static Map<String, Object> parameters ;

    @BeforeAll
    public static void setUp() {
        udfContext = new UDFContext();
        parameters = new HashMap<>();
        udfContext.setParameters(parameters);
        udfContext.setLookup_fields(Arrays.asList("ip"));
        udfContext.setOutput_fields(Arrays.asList("asn"));
    }



    @Test
    public void testInit(){
        AsnLookup asnLookup = new AsnLookup();
        udfContext.setLookup_fields(new ArrayList<>());
        udfContext.setParameters(new HashMap<>());
        udfContext.setParameters(null);
        Assertions.assertThrows(GrootStreamRuntimeException.class, () -> {
            asnLookup.open(null, udfContext);
        });
        udfContext.setParameters(new HashMap<>());
        udfContext.getParameters().put("option","IP_TO_ASN");
        Assertions.assertThrows(GrootStreamRuntimeException.class, () -> {
            asnLookup.open(null, udfContext);
        });

        udfContext.setLookup_fields(new ArrayList<>());
        udfContext.getLookup_fields().add("v1");
        udfContext.setOutput_fields(new ArrayList<>());
        udfContext.getOutput_fields().add("v2");
        udfContext.setParameters(new HashMap<>());
        udfContext.getParameters().put("option","other");
        Assertions.assertThrows(GrootStreamRuntimeException.class, () -> {
            asnLookup.open(null, udfContext);
        });

    }

    @Test
    public void testAsnLookupFunctionIpToAsn() throws Exception {

        udfContext.setParameters(new HashMap<>());
        udfContext.getParameters().put("option","IP_TO_ASN");
        udfContext.getParameters().put("vendor_id","tsg_asnlookup");
   /*     KnowledgeConfig knowledgeConfig = new KnowledgeConfig();
        knowledgeConfig.setName("tsg_asnlookup");
        knowledgeConfig.setType("asnlookup");
        knowledgeConfig.setFiles(Arrays.asList("7ce2f9890950ba90-fcc25696bf11a8a0","7ce2f9890950ba90-71f13b3736863ddb"));
        AsnKnowledgeBase asnKnowledgeBase = AsnKnowledgeBase.getInstance();
        asnKnowledgeBase.updateKnowledgeBase(knowledgeConfig);
        String asn  = AsnKnowledgeBase.getVenderWithAsnLookup()
                .get("tsg_asnlookup")
                .asnLookup("2600:1015:b002::");

        assertEquals("6167", asn);*/

    }


}
