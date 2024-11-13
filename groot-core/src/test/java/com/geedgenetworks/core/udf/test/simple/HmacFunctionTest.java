package com.geedgenetworks.core.udf.test.simple;

import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.core.udf.Hmac;
import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.connector.event.Event;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class HmacFunctionTest {

    private static final String SECRET_KEY = ".geedgenetworks.";
    private static final String DATA = "13812345678";
    private static UDFContext udfContext;

    @BeforeAll
    public static void setUp() {
        udfContext = new UDFContext();
        udfContext.setLookupFields(Collections.singletonList("phone_number"));
        udfContext.setOutputFields(Collections.singletonList("phone_number_mac"));
    }

    @Test
    public void testHmacAsBase64() {
        Map<String, Object> map = new HashMap<>();
        map.put("secret_key", SECRET_KEY);
        map.put("algorithm", "sha256");
        map.put("output_format", "base64");
        udfContext.setParameters(map);
        Hmac hmac = new Hmac();
        hmac.open(null, udfContext);
        Event event = new Event();
        Map<String, Object> extractedFields = new HashMap<>();
        extractedFields.put("phone_number", DATA);
        event.setExtractedFields(extractedFields);
        Event result1 = hmac.evaluate(event);
        assertEquals("zaj6UKovIsDahIBeRZ2PmgPIfDEr900F2xWu+iQfFrw=", result1.getExtractedFields().get("phone_number_mac"));
    }

    @Test
    public void testHmacAsHex() {
        Map<String, Object> map = new HashMap<>();
        map.put("secret_key", SECRET_KEY);
        map.put("algorithm", "sha256");
        map.put("output_format", "hex");
        udfContext.setParameters(map);
        Hmac hmac = new Hmac();
        hmac.open(null, udfContext);
        Event event = new Event();
        Map<String, Object> extractedFields = new HashMap<>();
        extractedFields.put("phone_number", DATA);
        event.setExtractedFields(extractedFields);
        Event result1 = hmac.evaluate(event);
        assertEquals("cda8fa50aa2f22c0da84805e459d8f9a03c87c312bf74d05db15aefa241f16bc", result1.getExtractedFields().get("phone_number_mac"));
    }

    @Test
    public void testHmacAlgorithm() {
        Map<String, Object> map = new HashMap<>();
        map.put("secret_key", SECRET_KEY);
        map.put("algorithm", "sm4");
        map.put("output_format", "base64");
        udfContext.setParameters(map);
        Hmac hmac = new Hmac();
        hmac.open(null, udfContext);
        Event event = new Event();
        Map<String, Object> extractedFields = new HashMap<>();
        extractedFields.put("phone_number", DATA);
        event.setExtractedFields(extractedFields);
        Event result = hmac.evaluate(event);
        assertEquals("QX1q4Y7y3quYCDje9BuSjg==", result.getExtractedFields().get("phone_number_mac"));

        map = new HashMap<>();
        map.put("secret_key", SECRET_KEY);
        map.put("algorithm", "sha1");
        map.put("output_format", "base64");
        udfContext.setParameters(map);
        hmac = new Hmac();
        hmac.open(null, udfContext);
        event.setExtractedFields(extractedFields);
        result = hmac.evaluate(event);
        assertEquals("NB1b1TsVZ95/0sE+d/6kdtyUFh0=", result.getExtractedFields().get("phone_number_mac"));

        map = new HashMap<>();
        map.put("secret_key", SECRET_KEY);
        map.put("algorithm", "sm3");
        map.put("output_format", "base64");
        udfContext.setParameters(map);
        hmac = new Hmac();
        hmac.open(null, udfContext);
        event.setExtractedFields(extractedFields);
        result = hmac.evaluate(event);
        assertEquals("BbQNpwLWE3rkaI1WlPBJgYeD14UyL2OwTxiEoTNA3UU=", result.getExtractedFields().get("phone_number_mac"));

        map = new HashMap<>();
        map.put("secret_key", SECRET_KEY);
        map.put("algorithm", "md5");
        map.put("output_format", "base64");
        udfContext.setParameters(map);
        hmac = new Hmac();
        hmac.open(null, udfContext);
        event.setExtractedFields(extractedFields);
        result = hmac.evaluate(event);
        assertEquals("BQZzRqD3ZR/nJsDIOO4dBg==", result.getExtractedFields().get("phone_number_mac"));

        map = new HashMap<>();
        map.put("secret_key", SECRET_KEY);
        map.put("algorithm", "sha512");
        map.put("output_format", "base64");
        udfContext.setParameters(map);
        hmac = new Hmac();
        hmac.open(null, udfContext);
        event.setExtractedFields(extractedFields);
        result = hmac.evaluate(event);
        assertEquals("DWrndzlcqf2qvFTbuDC1gZCGmRhuAUayfsxEqr2ZlpY/QOr9HgGUZNOfytRfA4VT8OZK0BwHwcAg5pgGBvPQ4A==", result.getExtractedFields().get("phone_number_mac"));
    }

    @Test
    public void testHmacError() {
        Map<String, Object> map = new HashMap<>();
        map.put("secret_key", SECRET_KEY);
        map.put("algorithm", "sha256");
        map.put("output_format", "hex");
        udfContext.setParameters(map);
        Hmac hmac = new Hmac();
        udfContext.getParameters().remove("secret_key");
        assertThrows(GrootStreamRuntimeException.class, () -> hmac.open(null, udfContext));
    }
}
