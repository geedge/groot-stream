package com.geedgenetworks.core.udf.cn;

import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.event.Event;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.geedgenetworks.core.udf.cn.LookupTestUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

class AppCategoryLookupTest {

    private static AppCategoryLookup appCategoryLookup;

    @BeforeAll
    static void setUp() {
        UDFContext udfContext = new UDFContext();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("kb_name", kbName);
        Map<String, String> fieldMapping = new HashMap<>();
        fieldMapping.put("CATEGORY", "app_category");
        fieldMapping.put("SUBCATEGORY", "app_subcategory");
        fieldMapping.put("COMPANY", "app_company");
        fieldMapping.put("COMPANY_CATEGORY", "app_company_category");
        parameters.put("field_mapping", fieldMapping);
        udfContext.setParameters(parameters);
        udfContext.setLookupFields(Collections.singletonList("app"));

        RuntimeContext runtimeContext = mockRuntimeContext();

        String content = "app_name,app_category,app_subcategory,app_company,app_company_category\nqq,collaboration,instant-messaging,Tencent,TencentCategory";
        mockKnowledgeBaseHandler(content);

        appCategoryLookup = new AppCategoryLookup();
        appCategoryLookup.open(runtimeContext, udfContext);
    }

    @Test
    void evaluate() {
        Event event = new Event();
        Map<String, Object> fields = new HashMap<>();
        fields.put("app", "qq");
        event.setExtractedFields(fields);
        Event evaluate = appCategoryLookup.evaluate(event);
        assertEquals("collaboration", evaluate.getExtractedFields().get("app_category"));
        assertEquals("instant-messaging", evaluate.getExtractedFields().get("app_subcategory"));
        assertEquals("Tencent", evaluate.getExtractedFields().get("app_company"));
        assertEquals("TencentCategory", evaluate.getExtractedFields().get("app_company_category"));
    }

    @AfterEach
    void afterAll() {
        clearState();
    }
}