package com.geedgenetworks.core.udf.test.simple;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.udf.UDFContext;
import com.geedgenetworks.core.udf.Rename;
import com.geedgenetworks.core.udf.StringJoiner;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class StringJoinerFunctionTest {
    private static UDFContext udfContext;

    @BeforeAll
    public static void setUp() {
        udfContext = new UDFContext();
        udfContext.setLookupFields(Arrays.asList("server_ip", "client_ip"));
        udfContext.setOutputFields(Collections.singletonList("ip_string"));
        Map<String, Object> params = new HashMap<>();
        params.put("separator",",");
        params.put("prefix","[");
        params.put("suffix","]");
        udfContext.setParameters(params);
    }

    // 测试方法

    @Test
    public void testStringJoinerFunction() {
        StringJoiner stringJoiner = new StringJoiner();
        stringJoiner.open(null, udfContext);
        Event event = new Event();
        Map<String, Object> extractedFields = new HashMap<>();
        extractedFields.put("server_ip", "192.168.1.1");
        extractedFields.put("client_ip", "192.168.2.1");
        event.setExtractedFields(extractedFields);
        Event result = stringJoiner.evaluate(event);
        assertEquals("[192.168.1.1,192.168.2.1]", result.getExtractedFields().get("ip_string"));
    }
}
