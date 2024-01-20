package com.geedgenetworks.core.udf.test.simple;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.udf.UDFContext;
import com.geedgenetworks.core.udf.JsonExtract;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class JsonExtractFunctionTest {

    private static UDFContext udfContext;
    @BeforeAll
    public static void setUp() {
        udfContext = new UDFContext();
    }

    @Test
    public void testInit(){
        JsonExtract jsonExtract = new JsonExtract();
        udfContext.setLookup_fields(new ArrayList<>());
        udfContext.setParameters(new HashMap<>());
        udfContext.setParameters(null);
        Assertions.assertThrows(GrootStreamRuntimeException.class, () -> {
            jsonExtract.open(null, udfContext);
        });
        udfContext.setParameters(new HashMap<>());
        udfContext.getParameters().put("value_expression","$.tags[?(@.tag=='device_group')][0].value");
        Assertions.assertThrows(GrootStreamRuntimeException.class, () -> {
            jsonExtract.open(null, udfContext);
        });
        udfContext.setParameters(new HashMap<>());
        udfContext.getParameters().put("other","other");
        Assertions.assertThrows(GrootStreamRuntimeException.class, () -> {
            jsonExtract.open(null, udfContext);
        });

    }

    @Test
    public void testJsonExtractFunction() {


        JsonExtract jsonExtract = new JsonExtract();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("value_expression","$.tags[?(@.tag=='device_group')][0].value");
        udfContext.setParameters(parameters);
        udfContext.setLookup_fields(Collections.singletonList("device_tag"));
        udfContext.setOutput_fields(Collections.singletonList("device_group"));
        jsonExtract.open(null, udfContext);
        Event event = new Event();
        String jsonString = "{\"device_tag\":\"{\\\"tags\\\":[{\\\"tag\\\":\\\"data_center\\\",\\\"value\\\":\\\"center-xxg-tsgx\\\"},{\\\"tag\\\":\\\"device_group\\\",\\\"value\\\":\\\"group-xxg-tsgx\\\"}]}\"}";
        Map<String, Object> extractedFields = JSON.parseObject(jsonString, new TypeReference<Map<String, Object>>() {});
        event.setExtractedFields(extractedFields);
        Event result1 = jsonExtract.evaluate(event);
        assertEquals("group-xxg-tsgx", result1.getExtractedFields().get("device_group"));
    }
}
