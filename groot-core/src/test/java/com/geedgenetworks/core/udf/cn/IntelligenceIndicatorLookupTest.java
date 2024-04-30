package com.geedgenetworks.core.udf.cn;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.udf.UDFContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static com.geedgenetworks.core.udf.cn.LookupTestUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author gujinkai
 * @version 1.0
 * @date 2024/4/7 14:26
 */
public class IntelligenceIndicatorLookupTest {

    private static IntelligenceIndicatorLookup intelligenceIndicatorLookup;

    private static RuntimeContext runtimeContext;

    @BeforeEach
    void setUp() {
        runtimeContext = mockRuntimeContext();

        String content = "type,ip_addr_format,ip1,ip2,domain,tags\nIP,CIDR,116.178.65.0,25,ali.com,\"阿里1,云服务1\"\nDomain,CIDR,116.178.65.0,25,$ali.com,\"阿里2,云服务2\"\nDomain,CIDR,116.178.65.0,25,*baidu.com,\"阿里3,云服务3\"\nIP,Single,116.178.65.64,116.178.65.64,ali.com,\"test\"";
        mockKnowledgeBaseHandler(content);

        intelligenceIndicatorLookup = new IntelligenceIndicatorLookup();
    }

    @Test
    void evaluate1() {
        UDFContext udfContext = new UDFContext();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("kb_name", kbName);
        parameters.put("option", "IP_TO_TAG");
        udfContext.setParameters(parameters);
        udfContext.setLookup_fields(Collections.singletonList("server_ip"));
        udfContext.setOutput_fields(Collections.singletonList("server_ip_tags"));
        intelligenceIndicatorLookup.open(runtimeContext, udfContext);

        Event event = new Event();
        Map<String, Object> fields = new HashMap<>();
        fields.put("server_ip", "116.178.65.100");
        event.setExtractedFields(fields);
        Event evaluate = intelligenceIndicatorLookup.evaluate(event);
        assertEquals(Arrays.asList("阿里1", "云服务1"), evaluate.getExtractedFields().get("server_ip_tags"));
    }

    @Test
    void evaluate2() {
        UDFContext udfContext = new UDFContext();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("kb_name", kbName);
        parameters.put("option", "IP_TO_TAG");
        udfContext.setParameters(parameters);
        udfContext.setLookup_fields(Collections.singletonList("server_ip"));
        udfContext.setOutput_fields(Collections.singletonList("server_ip_tags"));
        intelligenceIndicatorLookup.open(runtimeContext, udfContext);

        Event event = new Event();
        Map<String, Object> fields = new HashMap<>();
        fields.put("server_ip", "116.178.65.100");
        ArrayList<String> tags = new ArrayList<>();
        tags.add("test");
        tags.add("test1");
        fields.put("server_ip_tags", tags);
        event.setExtractedFields(fields);
        Event evaluate = intelligenceIndicatorLookup.evaluate(event);
        assertEquals(Arrays.asList("test", "test1", "阿里1", "云服务1"), evaluate.getExtractedFields().get("server_ip_tags"));
    }

    @Test
    void evaluate3() {
        UDFContext udfContext = new UDFContext();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("kb_name", kbName);
        parameters.put("option", "DOMAIN_TO_TAG");
        udfContext.setParameters(parameters);
        udfContext.setLookup_fields(Collections.singletonList("domain"));
        udfContext.setOutput_fields(Collections.singletonList("domain_tags"));
        intelligenceIndicatorLookup.open(runtimeContext, udfContext);

        Event event = new Event();
        Map<String, Object> fields = new HashMap<>();
        fields.put("domain", "ali.com");
        event.setExtractedFields(fields);
        Event evaluate = intelligenceIndicatorLookup.evaluate(event);
        assertEquals(Arrays.asList("阿里2", "云服务2"), evaluate.getExtractedFields().get("domain_tags"));
    }

    @Test
    void evaluate4() {
        UDFContext udfContext = new UDFContext();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("kb_name", kbName);
        parameters.put("option", "DOMAIN_TO_TAG");
        udfContext.setParameters(parameters);
        udfContext.setLookup_fields(Collections.singletonList("domain"));
        udfContext.setOutput_fields(Collections.singletonList("domain_tags"));
        intelligenceIndicatorLookup.open(runtimeContext, udfContext);

        Event event = new Event();
        Map<String, Object> fields = new HashMap<>();
        fields.put("domain", "ali.com");
        ArrayList<String> tags = new ArrayList<>();
        tags.add("test");
        tags.add("test1");
        fields.put("domain_tags", tags);
        event.setExtractedFields(fields);
        Event evaluate = intelligenceIndicatorLookup.evaluate(event);
        assertEquals(Arrays.asList("test", "test1", "阿里2", "云服务2"), evaluate.getExtractedFields().get("domain_tags"));
    }

    @Test
    void evaluate5() {
        UDFContext udfContext = new UDFContext();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("kb_name", kbName);
        parameters.put("option", "DOMAIN_TO_TAG");
        udfContext.setParameters(parameters);
        udfContext.setLookup_fields(Collections.singletonList("domain"));
        udfContext.setOutput_fields(Collections.singletonList("domain_tags"));
        intelligenceIndicatorLookup.open(runtimeContext, udfContext);

        Event event = new Event();
        Map<String, Object> fields = new HashMap<>();
        fields.put("domain", "test.baidu.com");
        event.setExtractedFields(fields);
        Event evaluate = intelligenceIndicatorLookup.evaluate(event);
        assertEquals(Arrays.asList("阿里3", "云服务3"), evaluate.getExtractedFields().get("domain_tags"));
    }

    @Test
    void evaluate6() {
        UDFContext udfContext = new UDFContext();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("kb_name", kbName);
        parameters.put("option", "IP_TO_TAG");
        udfContext.setParameters(parameters);
        udfContext.setLookup_fields(Collections.singletonList("server_ip"));
        udfContext.setOutput_fields(Collections.singletonList("server_ip_tags"));
        intelligenceIndicatorLookup.open(runtimeContext, udfContext);

        Event event = new Event();
        Map<String, Object> fields = new HashMap<>();
        fields.put("server_ip", "116.178.65.64");
        event.setExtractedFields(fields);
        Event evaluate1 = intelligenceIndicatorLookup.evaluate(event);
        assertEquals(Arrays.asList("阿里1", "云服务1", "test"), evaluate1.getExtractedFields().get("server_ip_tags"));
    }

    @AfterEach
    void afterAll() {
        clearState();
    }
}
