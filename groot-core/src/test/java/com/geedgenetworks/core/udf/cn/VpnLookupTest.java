package com.geedgenetworks.core.udf.cn;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.udf.UDFContext;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.geedgenetworks.core.udf.cn.LookupTestUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

class VpnLookupTest {

    private VpnLookup vpnLookup;

    @BeforeEach
    void setUp() {
        vpnLookup = new VpnLookup();
    }

    @Test
    void evaluateIp() {
        UDFContext udfContext = new UDFContext();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("kb_name", kbName);
        parameters.put("option", "IP_TO_VPN");
        udfContext.setParameters(parameters);
        udfContext.setLookupFields(Collections.singletonList("server_ip"));
        udfContext.setOutputFields(Collections.singletonList("server_vpn_service_name"));

        RuntimeContext runtimeContext = mockRuntimeContext();

        String content = "addr_format,ip1,ip2,vpn_service_name\nCIDR,116.178.65.0,25,阿里";
        mockKnowledgeBaseHandler(content);

        vpnLookup.open(runtimeContext, udfContext);

        Event event = new Event();
        Map<String, Object> fields = new HashMap<>();
        fields.put("server_ip", "116.178.65.100");
        event.setExtractedFields(fields);
        Event evaluate = vpnLookup.evaluate(event);
        assertEquals("阿里", evaluate.getExtractedFields().get("server_vpn_service_name"));
    }

    @Test
    void evaluateDomain() {
        UDFContext udfContext = new UDFContext();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("kb_name", kbName);
        parameters.put("option", "DOMAIN_TO_VPN");
        udfContext.setParameters(parameters);
        udfContext.setLookupFields(Collections.singletonList("domain"));
        udfContext.setOutputFields(Collections.singletonList("domain_vpn_service_name"));

        RuntimeContext runtimeContext = mockRuntimeContext();

        String content = "domain,vpn_service_name\nblade43.singapore-rack443.nodes.gen4.ninja,cyberghostvpn";
        mockKnowledgeBaseHandler(content);

        vpnLookup.open(runtimeContext, udfContext);

        Event event = new Event();
        Map<String, Object> fields = new HashMap<>();
        fields.put("domain", "test.blade43.singapore-rack443.nodes.gen4.ninja");
        event.setExtractedFields(fields);
        Event evaluate = vpnLookup.evaluate(event);
        assertEquals("cyberghostvpn", evaluate.getExtractedFields().get("domain_vpn_service_name"));
    }

    @AfterEach
    void afterAll() {
        clearState();
    }
}