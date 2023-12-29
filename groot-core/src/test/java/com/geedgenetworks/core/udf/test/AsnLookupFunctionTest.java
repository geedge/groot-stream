package com.geedgenetworks.core.udf.test;

import com.geedgenetworks.common.config.CommonConfig;
import com.geedgenetworks.common.config.KnowledgeBaseConfig;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;
import com.geedgenetworks.core.udf.AsnLookup;
import com.geedgenetworks.core.udf.FromUnixTimestamp;
import com.geedgenetworks.core.utils.KnowlegdeBase.AsnKnowledgeBase;
import com.geedgenetworks.core.utils.KnowlegdeBase.KnowledgeBaseUpdateJob;
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
    public void testAsnLookupFunctionIpToAsn() throws Exception {


        CommonConfig commonConfig = new CommonConfig();
        List<KnowledgeBaseConfig> knowledgeBaseConfigList = new ArrayList<>();
        KnowledgeBaseConfig knowledgeBaseConfig = new KnowledgeBaseConfig();
        knowledgeBaseConfig.setName("tsg_asnlookup");
        Map<String, String> parameters = new HashMap<>();
        parameters.put("fs_type","hos");
        parameters.put("fs_default_path","http://192.168.44.67:9999/v1/knowledge_base?kb_id=");
        knowledgeBaseConfig.setProperties(parameters);
        knowledgeBaseConfig.setType("asnlookup");
        knowledgeBaseConfig.setFiles(Collections.singletonList("f9f6bc91-2142-4673-8249-e097c00fe1ea"));
        knowledgeBaseConfigList.add(knowledgeBaseConfig);
        commonConfig.setKnowledgeBaseConfig(knowledgeBaseConfigList);
        KnowledgeBaseUpdateJob.initKnowledgeBase("tsg_asnlookup", AsnKnowledgeBase.getInstance(), commonConfig);
        String asn = AsnKnowledgeBase.getVenderWithAsnLookup()
                .get("tsg_asnlookup")
                .asnLookup("2600:1015:b002::");
        assertEquals("6167", asn);

    }


}
