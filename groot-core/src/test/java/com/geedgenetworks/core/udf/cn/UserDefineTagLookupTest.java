package com.geedgenetworks.core.udf.cn;

import com.geedgenetworks.spi.common.udf.UDFContext;
import com.geedgenetworks.spi.table.event.Event;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.geedgenetworks.core.udf.cn.LookupTestUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

class UserDefineTagLookupTest {
    private static UserDefineTagLookup userDefineTagLookup;

    private static RuntimeContext runtimeContext;

    @BeforeEach
    void setUp() {
        runtimeContext = mockRuleRuntimeContext();

        userDefineTagLookup = new UserDefineTagLookup();
    }

    @Test
    void evaluateIp() {
        String content = "addr_format,ip1,ip2,tag_value\nCIDR,103.3.136.128,25,4G";
        mockMultipleKnowledgeBaseHandler(content);

        UDFContext udfContext = new UDFContext();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("kb_name", kbName);
        parameters.put("option", "IP_TO_TAG");
        udfContext.setParameters(parameters);
        udfContext.setLookupFields(Collections.singletonList("client_ip"));
        udfContext.setOutputFields(Collections.singletonList("client_ip_tags"));
        userDefineTagLookup.open(runtimeContext, udfContext);

        Event event = new Event();
        Map<String, Object> fields = new HashMap<>();
        fields.put("client_ip", "103.3.136.130");
        event.setExtractedFields(fields);
        Event evaluate = userDefineTagLookup.evaluate(event);
        assertEquals(Arrays.asList("4G"), evaluate.getExtractedFields().get("client_ip_tags"));
    }

    @Test
    void evaluateDomainFuzzy() {
        String content = "domain,tag_value\n*baidu.com,test_tag";
        mockMultipleKnowledgeBaseHandler(content);

        UDFContext udfContext = new UDFContext();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("kb_name", kbName);
        parameters.put("option", "DOMAIN_TO_TAG");
        udfContext.setParameters(parameters);
        udfContext.setLookupFields(Collections.singletonList("domain"));
        udfContext.setOutputFields(Collections.singletonList("domain_tags"));
        userDefineTagLookup.open(runtimeContext, udfContext);

        Event event = new Event();
        Map<String, Object> fields = new HashMap<>();
        fields.put("domain", "www.baidu.com");
        event.setExtractedFields(fields);
        Event evaluate = userDefineTagLookup.evaluate(event);
        assertEquals(Arrays.asList("test_tag"), evaluate.getExtractedFields().get("domain_tags"));
    }

    @Test
    void evaluateDomainFull() {
        String content = "domain,tag_value\n$baidu.com,test_tag";
        mockMultipleKnowledgeBaseHandler(content);

        UDFContext udfContext = new UDFContext();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("kb_name", kbName);
        parameters.put("option", "DOMAIN_TO_TAG");
        udfContext.setParameters(parameters);
        udfContext.setLookupFields(Collections.singletonList("domain"));
        udfContext.setOutputFields(Collections.singletonList("domain_tags"));
        userDefineTagLookup.open(runtimeContext, udfContext);

        Event event = new Event();
        Map<String, Object> fields = new HashMap<>();
        fields.put("domain", "baidu.com");
        event.setExtractedFields(fields);
        Event evaluate = userDefineTagLookup.evaluate(event);
        assertEquals(Arrays.asList("test_tag"), evaluate.getExtractedFields().get("domain_tags"));
    }

    @Test
    void evaluateApp() {
        String content = "app_name,tag_value\nbaidu,test_tag";
        mockMultipleKnowledgeBaseHandler(content);

        UDFContext udfContext = new UDFContext();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("kb_name", kbName);
        parameters.put("option", "APP_TO_TAG");
        udfContext.setParameters(parameters);
        udfContext.setLookupFields(Collections.singletonList("app"));
        udfContext.setOutputFields(Collections.singletonList("app_tags"));
        userDefineTagLookup.open(runtimeContext, udfContext);

        Event event = new Event();
        Map<String, Object> fields = new HashMap<>();
        fields.put("app", "baidu");
        event.setExtractedFields(fields);
        Event evaluate = userDefineTagLookup.evaluate(event);
        assertEquals(Arrays.asList("test_tag"), evaluate.getExtractedFields().get("app_tags"));
    }

    @AfterEach
    void afterAll() {
        clearState();
    }
}