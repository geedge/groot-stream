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

class IdcRenterLookupTest {

    private static IdcRenterLookup idcRenterLookup;

    @BeforeAll
    static void setUp() {
        UDFContext udfContext = new UDFContext();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("kb_name", kbName);
        udfContext.setParameters(parameters);
        udfContext.setLookup_fields(Collections.singletonList("server_ip"));
        udfContext.setOutput_fields(Collections.singletonList("server_idc_renter"));

        RuntimeContext runtimeContext = mockRuntimeContext();

        String content = "addr_format,ip1,ip2,idc_renter\nCIDR,116.178.65.0,25,阿里";
        mockKnowledgeBaseHandler(content);

        idcRenterLookup = new IdcRenterLookup();
        idcRenterLookup.open(runtimeContext, udfContext);
    }

    @Test
    void evaluate() {
        Event event = new Event();
        Map<String, Object> fields = new HashMap<>();
        fields.put("server_ip", "116.178.65.100");
        event.setExtractedFields(fields);
        Event evaluate = idcRenterLookup.evaluate(event);
        assertEquals("阿里", evaluate.getExtractedFields().get("server_idc_renter"));
    }

    @AfterEach
    void afterAll() {
        clearState();
    }
}