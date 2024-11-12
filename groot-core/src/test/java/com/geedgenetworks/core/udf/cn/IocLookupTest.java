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

class IocLookupTest {

    private static IocLookup iocLookup;

    private static RuntimeContext runtimeContext;

    @BeforeEach
    void setUp() {
        runtimeContext = mockRuleRuntimeContext();

        String content = "ioc_type,ioc_value,malware_name\nip,2.50.159.104,QakBot\ndomain,72ioey.badomininter.cloud,Astaroth";
        mockKnowledgeBaseHandler(content);

        iocLookup = new IocLookup();
    }

    @Test
    void evaluateIp() {
        UDFContext udfContext = new UDFContext();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("kb_name", kbName);
        parameters.put("option", "IP_TO_MALWARE");
        udfContext.setParameters(parameters);
        udfContext.setLookupFields(Collections.singletonList("server_ip"));
        udfContext.setOutputFields(Collections.singletonList("server_malware"));
        iocLookup.open(runtimeContext, udfContext);

        Event event = new Event();
        Map<String, Object> fields = new HashMap<>();
        fields.put("server_ip", "2.50.159.104");
        event.setExtractedFields(fields);
        Event evaluate = iocLookup.evaluate(event);
        assertEquals("QakBot", evaluate.getExtractedFields().get("server_malware"));
    }

    @Test
    void evaluateDomain() {
        UDFContext udfContext = new UDFContext();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("kb_name", kbName);
        parameters.put("option", "DOMAIN_TO_MALWARE");
        udfContext.setParameters(parameters);
        udfContext.setLookupFields(Collections.singletonList("domain"));
        udfContext.setOutputFields(Collections.singletonList("domain_malware"));
        iocLookup.open(runtimeContext, udfContext);

        Event event = new Event();
        Map<String, Object> fields = new HashMap<>();
        fields.put("domain", "test.72ioey.badomininter.cloud");
        event.setExtractedFields(fields);
        Event evaluate = iocLookup.evaluate(event);
        assertEquals("Astaroth", evaluate.getExtractedFields().get("domain_malware"));
    }

    @AfterEach
    void afterAll() {
        clearState();
    }
}