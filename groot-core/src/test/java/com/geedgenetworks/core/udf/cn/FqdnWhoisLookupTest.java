package com.geedgenetworks.core.udf.cn;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.udf.UDFContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.geedgenetworks.core.udf.cn.LookupTestUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

class FqdnWhoisLookupTest {

    private static FqdnWhoisLookup fqdnWhoisLookup;

    @BeforeAll
    static void setUp() {
        UDFContext udfContext = new UDFContext();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("kb_name", kbName);
        udfContext.setParameters(parameters);
        udfContext.setLookup_fields(Collections.singletonList("domain"));
        udfContext.setOutput_fields(Collections.singletonList("domain_whois_org"));

        RuntimeContext runtimeContext = mockRuntimeContext();

        String content = "fqdn,whois_registrant_org\nschoolexpress.com,comped";
        mockKnowledgeBaseHandler(content);

        fqdnWhoisLookup = new FqdnWhoisLookup();
        fqdnWhoisLookup.open(runtimeContext, udfContext);
    }

    @Test
    void evaluate() {
        Event event = new Event();
        Map<String, Object> fields = new HashMap<>();
        fields.put("domain", "www.schoolexpress.com");
        event.setExtractedFields(fields);
        Event evaluate = fqdnWhoisLookup.evaluate(event);
        assertEquals("comped", evaluate.getExtractedFields().get("domain_whois_org"));
    }

    @AfterEach
    void afterAll() {
        clearState();
    }
}