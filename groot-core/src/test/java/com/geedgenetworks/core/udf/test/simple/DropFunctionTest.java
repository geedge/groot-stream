package com.geedgenetworks.core.udf.test.simple;

import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;
import com.geedgenetworks.core.udf.Drop;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DropFunctionTest {

    private static UDFContext udfContext;

    @BeforeAll
    public static void setUp() {
        udfContext = new UDFContext();
        udfContext.setFilter("map.common_schema_type == 'HTTP' && map.common_server_port > 60");
    }

    // 测试方法

    @Test
    public void testDropFunctionTrue() {
        Drop drop = new Drop();
        drop.open(null, udfContext);
        Event event = new Event();
        Map<String, Object> extractedFields = new HashMap<>();
        extractedFields.put("common_schema_type", "HTTP");
        extractedFields.put("common_server_port", 70);
        event.setExtractedFields(extractedFields);
        Event result = drop.evaluate(event);
        assertTrue(result.isFinal());
    }

    @Test
    public void testDropFunctionFalse() {
        Drop drop = new Drop();
        drop.open(null, udfContext);
        Event event = new Event();
        Map<String, Object> extractedFields = new HashMap<>();
        extractedFields.put("common_schema_type", "HTTP");
        extractedFields.put("common_server_port", 50);
        event.setExtractedFields(extractedFields);
        Event result = drop.evaluate(event);
        assertFalse(result.isFinal());
    }

}
