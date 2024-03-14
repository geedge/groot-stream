package com.geedgenetworks.core.udf.test;

import com.geedgenetworks.common.config.CommonConfig;
import com.geedgenetworks.common.config.KnowledgeBaseConfig;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.udf.UDFContext;
import com.geedgenetworks.core.udf.AsnLookup;
import com.geedgenetworks.core.udf.knowlegdebase.handler.AsnKnowledgeBaseHandler;
import com.geedgenetworks.core.udf.knowlegdebase.KnowledgeBaseUpdateJob;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AsnLookupFunctionTest {

    private static UDFContext udfContext;

    private static Map<String, Object> parameters ;

    @BeforeAll
    public static void setUp() {
        udfContext = new UDFContext();
        parameters = new HashMap<>();
        udfContext.setParameters(parameters);
        udfContext.setLookup_fields(Collections.singletonList("ip"));
        udfContext.setOutput_fields(Collections.singletonList("asn"));

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
    public void testAsnLookupFunction() throws Exception {


        CommonConfig commonConfig = new CommonConfig();
        List<KnowledgeBaseConfig> knowledgeBaseConfigList = new ArrayList<>();
        KnowledgeBaseConfig knowledgeBaseConfig = new KnowledgeBaseConfig();
        knowledgeBaseConfig.setName("tsg_ip_asn");
        Map<String, String> parameters = new HashMap<>();
        knowledgeBaseConfig.setProperties(parameters);
        knowledgeBaseConfig.setFsType("local");
        knowledgeBaseConfig.setFsPath("");
        knowledgeBaseConfig.setFiles(Collections.singletonList("src/test/resources/asn.mmdb"));
        knowledgeBaseConfigList.add(knowledgeBaseConfig);
        commonConfig.setKnowledgeBaseConfig(knowledgeBaseConfigList);
        KnowledgeBaseUpdateJob.registerKnowledgeBase(AsnKnowledgeBaseHandler.getInstance(), knowledgeBaseConfig);
        String asn = AsnKnowledgeBaseHandler.lookUp("tsg_ip_asn","IP_TO_ASN","12.16.98.1");
        assertEquals("31978", asn);

    }


}
