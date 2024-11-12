package com.geedgenetworks.core.udf.cn;

import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.connector.event.Event;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.geedgenetworks.core.udf.cn.LookupTestUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

class AnonymityLookupTest {

    private static AnonymityLookup anonymityLookup;

    private static RuntimeContext runtimeContext;

    @BeforeEach
    void setUp() {
        runtimeContext = mockRuleRuntimeContext();

        String content = "ioc_type,ioc_value,node_type\nip,92.230.220.168,tor\ndomain,v103.vipgorup.xyz,mtproxy";
        mockKnowledgeBaseHandler(content);

        anonymityLookup = new AnonymityLookup();
    }

    @Test
    void evaluateIp() {
        UDFContext udfContext = new UDFContext();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("kb_name", kbName);
        parameters.put("option", "IP_TO_NODE_TYPE");
        udfContext.setParameters(parameters);
        udfContext.setLookupFields(Collections.singletonList("server_ip"));
        udfContext.setOutputFields(Collections.singletonList("server_node_type"));
        anonymityLookup.open(runtimeContext, udfContext);

        Event event = new Event();
        Map<String, Object> fields = new HashMap<>();
        fields.put("server_ip", "92.230.220.168");
        event.setExtractedFields(fields);
        Event evaluate = anonymityLookup.evaluate(event);
        assertEquals("tor", evaluate.getExtractedFields().get("server_node_type"));
    }

    @Test
    void evaluateDomain() {
        UDFContext udfContext = new UDFContext();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("kb_name", kbName);
        parameters.put("option", "DOMAIN_TO_NODE_TYPE");
        udfContext.setParameters(parameters);
        udfContext.setLookupFields(Collections.singletonList("domain"));
        udfContext.setOutputFields(Collections.singletonList("domain_node_type"));
        anonymityLookup.open(runtimeContext, udfContext);

        Event event = new Event();
        Map<String, Object> fields = new HashMap<>();
        fields.put("domain", "v103.vipgorup.xyz");
        event.setExtractedFields(fields);
        Event evaluate = anonymityLookup.evaluate(event);
        assertEquals("mtproxy", evaluate.getExtractedFields().get("domain_node_type"));
    }

    @AfterEach
    void afterAll() {
        clearState();
    }
}