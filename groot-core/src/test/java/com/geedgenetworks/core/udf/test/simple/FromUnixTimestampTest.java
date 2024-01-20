package com.geedgenetworks.core.udf.test.simple;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.udf.UDFContext;
import com.geedgenetworks.core.udf.FromUnixTimestamp;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FromUnixTimestampTest {

    private static UDFContext udfContext;

    @BeforeAll
    public static void setUp() {
        udfContext = new UDFContext();
        udfContext.setLookup_fields(Arrays.asList("unixTimestamp"));
        udfContext.setOutput_fields(Arrays.asList("timestamp"));
    }
    @Test
    public void testFromUnixTimestampFunction() throws Exception {

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("precision", "seconds");
        parameters.put("timezone", "UTC");
        udfContext.setParameters(parameters);
        FromUnixTimestamp fromUnixTimestamp = new FromUnixTimestamp();
        fromUnixTimestamp.open(null, udfContext);
        Event event = new Event();
        Map<String, Object> extractedFields = new HashMap<>();
        extractedFields.put("unixTimestamp", 1577808000000L);
        event.setExtractedFields(extractedFields);
        Event result1 = fromUnixTimestamp.evaluate(event);
        assertEquals("2019-12-31 16:00:00", result1.getExtractedFields().get("timestamp"));
        parameters.put("precision", "milliseconds");
        fromUnixTimestamp.open(null, udfContext);
        Event result2 = fromUnixTimestamp.evaluate(event);
        assertEquals("2019-12-31 16:00:00:000", result2.getExtractedFields().get("timestamp"));

    }
}
