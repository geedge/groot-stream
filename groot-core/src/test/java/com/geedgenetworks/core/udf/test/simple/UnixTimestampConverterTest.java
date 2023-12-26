package com.geedgenetworks.core.udf.test.simple;

import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;
import com.geedgenetworks.core.udf.UnixTimestampConverter;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class UnixTimestampConverterTest {

    private static UDFContext udfContext;

    @BeforeAll
    public static void setUp() {
        udfContext = new UDFContext();
        udfContext.setLookup_fields(Arrays.asList("input"));
        udfContext.setOutput_fields(Arrays.asList("output"));
    }
    @Test
    public void testUnixTimestampFunctionMstoS() throws Exception {

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("precision", "seconds");
        udfContext.setParameters(parameters);
        UnixTimestampConverter unixTimestampConverter = new UnixTimestampConverter();
        unixTimestampConverter.open(null, udfContext);
        Event event = new Event();
        Map<String, Object> extractedFields = new HashMap<>();
        extractedFields.put("input", 1577808000000L);
        event.setExtractedFields(extractedFields);
        Event result1 = unixTimestampConverter.evaluate(event);
        assertEquals(1577808000L, result1.getExtractedFields().get("output"));


    }

    @Test
    public void testUnixTimestampFunctionStoMs() throws Exception {

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("precision", "milliseconds");
        udfContext.setParameters(parameters);
        UnixTimestampConverter unixTimestampConverter = new UnixTimestampConverter();
        unixTimestampConverter.open(null, udfContext);
        Event event = new Event();
        Map<String, Object> extractedFields = new HashMap<>();
        extractedFields.put("input", 1577808000L);
        event.setExtractedFields(extractedFields);
        Event result1 = unixTimestampConverter.evaluate(event);
        assertEquals(1577808000000L, result1.getExtractedFields().get("output"));


    }


    @Test
    public void testUnixTimestampFunctionStoS() throws Exception {

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("precision", "seconds");
        udfContext.setParameters(parameters);
        UnixTimestampConverter unixTimestampConverter = new UnixTimestampConverter();
        unixTimestampConverter.open(null, udfContext);
        Event event = new Event();
        Map<String, Object> extractedFields = new HashMap<>();
        extractedFields.put("input", 1577808000L);
        event.setExtractedFields(extractedFields);
        Event result1 = unixTimestampConverter.evaluate(event);
        assertEquals(1577808000L, result1.getExtractedFields().get("output"));


    }


    @Test
    public void testUnixTimestampFunctionMstoMs() throws Exception {

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("precision", "milliseconds");
        udfContext.setParameters(parameters);
        UnixTimestampConverter unixTimestampConverter = new UnixTimestampConverter();
        unixTimestampConverter.open(null, udfContext);
        Event event = new Event();
        Map<String, Object> extractedFields = new HashMap<>();
        extractedFields.put("input", 1577808000000L);
        event.setExtractedFields(extractedFields);
        Event result1 = unixTimestampConverter.evaluate(event);
        assertEquals(1577808000000L, result1.getExtractedFields().get("output"));


    }
}
