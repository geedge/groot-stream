package com.geedgenetworks.core.udf.test.simple;

import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;
import com.geedgenetworks.core.udf.DecodeBase64;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DecodeBase64FunctionTest {

    private static UDFContext udfContext;

    @BeforeAll
    public static void setUp() {
         udfContext = new UDFContext();
         udfContext.setLookup_fields(Arrays.asList("message", "charset"));
         udfContext.setOutput_fields(Collections.singletonList("decodeResult"));
    }
    @Test
    public void testDecodeBase64Function() {

        DecodeBase64 decodeBase64 = new DecodeBase64();
        decodeBase64.open(null, udfContext);
        Event event = new Event();
        Map<String, Object> extractedFields = new HashMap<>();
        extractedFields.put("message", "aGVsbG8=");
        extractedFields.put("charset", "UTF-8");
        event.setExtractedFields(extractedFields);
        Event result1 = decodeBase64.evaluate(event);
        assertEquals("hello", result1.getExtractedFields().get("decodeResult"));
        extractedFields.put("charset", "GBK");
        Event result2 = decodeBase64.evaluate(event);
        assertEquals("hello", result2.getExtractedFields().get("decodeResult"));
    }

    @Test
    public void testDecodeBase64Error() {


        DecodeBase64 decodeBase64 = new DecodeBase64();
        udfContext.setLookup_fields(Collections.singletonList("message"));
        udfContext.setOutput_fields(Collections.singletonList("decodeResult"));
        assertThrows(GrootStreamRuntimeException.class,
                () -> {
                    decodeBase64.open(null, udfContext);
                });
    }
}
