package com.geedgenetworks.core.udf.cn;

import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.connector.event.Event;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.geedgenetworks.core.udf.cn.LookupTestUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

class LinkDirectionLookupTest {

    private static LinkDirectionLookup linkDirectionLookup;

    @BeforeAll
    static void setUp() {
        UDFContext udfContext = new UDFContext();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("kb_name", kbName);
        udfContext.setParameters(parameters);
        udfContext.setLookupFields(Collections.singletonList("in_link_id"));
        udfContext.setOutputFields(Collections.singletonList("in_link_direction"));

        RuntimeContext runtimeContext = mockRuntimeContext();

        String content = "peer_city,link_id\n北京,1";
        mockKnowledgeBaseHandler(content);

        linkDirectionLookup = new LinkDirectionLookup();
        linkDirectionLookup.open(runtimeContext, udfContext);
    }

    @Test
    void evaluate() {
        Event event = new Event();
        Map<String, Object> fields = new HashMap<>();
        fields.put("in_link_id", "1");
        event.setExtractedFields(fields);
        Event evaluate = linkDirectionLookup.evaluate(event);
        assertEquals("北京", evaluate.getExtractedFields().get("in_link_direction"));
    }

    @AfterEach
    void afterAll() {
        clearState();
    }
}