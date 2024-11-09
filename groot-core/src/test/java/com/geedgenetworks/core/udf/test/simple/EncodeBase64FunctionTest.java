package com.geedgenetworks.core.udf.test.simple;

import com.geedgenetworks.core.udf.EncodeBase64;
import com.geedgenetworks.spi.common.udf.UDFContext;
import com.geedgenetworks.spi.table.event.Event;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class EncodeBase64FunctionTest {

    private static UDFContext udfContext;


    @Test
    public void testEncodeBase64FunctionForByte() {
        udfContext = new UDFContext();
        udfContext.setOutputFields(Collections.singletonList("encodeResult"));
        udfContext.setLookupFields(Collections.singletonList("name"));
        Map<String,Object> map = new HashMap<>();
        map.put("input_type","byte_array");
        udfContext.setParameters(map);
        EncodeBase64 encodeBase64 = new EncodeBase64();
        encodeBase64.open(null, udfContext);
        Event event = new Event();
        Map<String, Object> extractedFields = new HashMap<>();
        extractedFields.put("name", "hello".getBytes(StandardCharsets.UTF_8));
        event.setExtractedFields(extractedFields);
        Event result1 = encodeBase64.evaluate(event);
        assertEquals("aGVsbG8=", result1.getExtractedFields().get("encodeResult"));



    }

    @Test
    public void testEncodeBase64FunctionForString() {

        udfContext = new UDFContext();
        udfContext.setOutputFields(Collections.singletonList("encodeResult"));
        udfContext.setLookupFields(Collections.singletonList("name"));
        Map<String,Object> map = new HashMap<>();
        map.put("input_type","string");
        udfContext.setParameters(map);
        EncodeBase64 encodeBase64 = new EncodeBase64();
        encodeBase64.open(null, udfContext);
        Event event = new Event();
        Map<String, Object> extractedFields = new HashMap<>();
        extractedFields.put("name", "hello");
        event.setExtractedFields(extractedFields);
        Event result1 = encodeBase64.evaluate(event);
        assertEquals("aGVsbG8=", result1.getExtractedFields().get("encodeResult"));

    }
}
