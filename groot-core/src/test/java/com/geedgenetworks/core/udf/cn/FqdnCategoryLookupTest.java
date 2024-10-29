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

class FqdnCategoryLookupTest {

    private static FqdnCategoryLookup fqdnCategoryLookup;

    @BeforeAll
    static void setUp() {
        UDFContext udfContext = new UDFContext();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("kb_name", kbName);
        Map<String, String> fieldMapping = new HashMap<>();
        fieldMapping.put("NAME", "domain_category_name");
        fieldMapping.put("GROUP", "domain_category_group");
        fieldMapping.put("REPUTATION_LEVEL", "domain_reputation_level");
        parameters.put("field_mapping", fieldMapping);
        udfContext.setParameters(parameters);
        udfContext.setLookupFields(Collections.singletonList("domain"));

        RuntimeContext runtimeContext = mockRuntimeContext();

        String content = "fqdn,category_name,category_group,reputation_level\ndouralquran.com,Parked Domains,Sensitive,Trustworthy";
        mockKnowledgeBaseHandler(content);

        fqdnCategoryLookup = new FqdnCategoryLookup();
        fqdnCategoryLookup.open(runtimeContext, udfContext);
    }

    @Test
    void evaluate() {
        Event event = new Event();
        Map<String, Object> fields = new HashMap<>();
        fields.put("domain", "www.douralquran.com");
        event.setExtractedFields(fields);
        Event evaluate = fqdnCategoryLookup.evaluate(event);
        assertEquals("Parked Domains", evaluate.getExtractedFields().get("domain_category_name"));
        assertEquals("Sensitive", evaluate.getExtractedFields().get("domain_category_group"));
        assertEquals("Trustworthy", evaluate.getExtractedFields().get("domain_reputation_level"));
    }

    @AfterEach
    void afterAll() {
        clearState();
    }
}