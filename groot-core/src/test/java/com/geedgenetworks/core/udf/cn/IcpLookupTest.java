package com.geedgenetworks.core.udf.cn;

import com.geedgenetworks.spi.common.udf.UDFContext;
import com.geedgenetworks.spi.table.event.Event;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.geedgenetworks.core.udf.cn.LookupTestUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

class IcpLookupTest {

    private static IcpLookup icpLookup;

    @BeforeAll
    static void setUp() {
        UDFContext udfContext = new UDFContext();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("kb_name", kbName);
        udfContext.setParameters(parameters);
        udfContext.setLookupFields(Collections.singletonList("domain"));
        udfContext.setOutputFields(Collections.singletonList("domain_icp_company_name"));

        RuntimeContext runtimeContext = mockRuntimeContext();

        String content = "fqdn,icp_company_name\ndreamtec-solutions.com.cn,上海骏品电子科技有限公司";
        mockKnowledgeBaseHandler(content);

        icpLookup = new IcpLookup();
        icpLookup.open(runtimeContext, udfContext);
    }

    @Test
    void evaluate() {
        Event event = new Event();
        Map<String, Object> fields = new HashMap<>();
        fields.put("domain", "www.dreamtec-solutions.com.cn");
        event.setExtractedFields(fields);
        Event evaluate = icpLookup.evaluate(event);
        assertEquals("上海骏品电子科技有限公司", evaluate.getExtractedFields().get("domain_icp_company_name"));
    }

    @AfterEach
    void afterAll() {
        clearState();
    }
}