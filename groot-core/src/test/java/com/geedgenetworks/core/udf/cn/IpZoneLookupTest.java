package com.geedgenetworks.core.udf.cn;

import com.geedgenetworks.spi.common.udf.UDFContext;
import com.geedgenetworks.spi.table.event.Event;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.geedgenetworks.core.udf.cn.LookupTestUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

class IpZoneLookupTest {

    private static IpZoneLookup ipZoneLookup;

    @BeforeEach
    void setUp() {
        UDFContext udfContext = new UDFContext();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("kb_name", kbName);
        udfContext.setParameters(parameters);
        udfContext.setLookupFields(Collections.singletonList("client_ip"));
        udfContext.setOutputFields(Collections.singletonList("client_zone"));

        RuntimeContext runtimeContext = mockRuntimeContext();

        String content = "addr_format,ip1,ip2\nCIDR,116.178.65.0,25";
        mockKnowledgeBaseHandler(content);

        ipZoneLookup = new IpZoneLookup();
        ipZoneLookup.open(runtimeContext, udfContext);
    }

    @Test
    void evaluateInternal() {
        Event event = new Event();
        Map<String, Object> fields = new HashMap<>();
        fields.put("client_ip", "116.178.65.100");
        event.setExtractedFields(fields);
        Event evaluate = ipZoneLookup.evaluate(event);
        assertEquals("internal", evaluate.getExtractedFields().get("client_zone"));
    }

    @Test
    void evaluateExternal() {
        Event event = new Event();
        Map<String, Object> fields = new HashMap<>();
        fields.put("client_ip", "116.178.64.100");
        event.setExtractedFields(fields);
        Event evaluate = ipZoneLookup.evaluate(event);
        assertEquals("external", evaluate.getExtractedFields().get("client_zone"));
    }

    @AfterEach
    void afterAll() {
        clearState();
    }
}