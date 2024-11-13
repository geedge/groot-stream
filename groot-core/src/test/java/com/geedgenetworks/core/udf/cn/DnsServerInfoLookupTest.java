package com.geedgenetworks.core.udf.cn;

import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.connector.event.Event;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.geedgenetworks.core.udf.cn.LookupTestUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

class DnsServerInfoLookupTest {

    private static DnsServerInfoLookup dnsServerInfoLookup;

    @BeforeAll
    static void setUp() {
        UDFContext udfContext = new UDFContext();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("kb_name", kbName);
        udfContext.setParameters(parameters);
        udfContext.setLookupFields(Collections.singletonList("server_ip"));
        udfContext.setOutputFields(Collections.singletonList("server_dns_server"));

        RuntimeContext runtimeContext = mockRuntimeContext();

        String content = "ip_addr,dns_server_role\n111.196.123.207,FWDNS";
        mockKnowledgeBaseHandler(content);

        dnsServerInfoLookup = new DnsServerInfoLookup();
        dnsServerInfoLookup.open(runtimeContext, udfContext);
    }

    @Test
    void evaluate() {
        Event event = new Event();
        Map<String, Object> fields = new HashMap<>();
        fields.put("server_ip", "111.196.123.207");
        event.setExtractedFields(fields);
        Event evaluate = dnsServerInfoLookup.evaluate(event);
        assertEquals(Arrays.asList("FWDNS"), evaluate.getExtractedFields().get("server_dns_server"));
    }

    @AfterEach
    void afterAll() {
        clearState();
    }
}