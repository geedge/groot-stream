package com.geedgenetworks.core.udf.test.simple;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.udf.UDFContext;
import com.geedgenetworks.core.udf.uuid.UUID;
import com.geedgenetworks.core.udf.uuid.UUIDv5;
import com.geedgenetworks.core.udf.uuid.UUIDv7;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class UUIDTest {

    private UDFContext udfContext;
    private Map<String, Object> parameters ;


    @Test
    public void testInit(){
        udfContext = new UDFContext();
        UUIDv5 uuidv5 = new UUIDv5();
        parameters =  new HashMap<>();
        udfContext.setParameters(parameters);
        udfContext.setLookupFields(List.of("client_ip","server_ip"));
        udfContext.setOutputFields(Collections.singletonList("uuid"));
        parameters.put("namespace","NAMESPACE_IP_1");
        Assertions.assertThrows(GrootStreamRuntimeException.class, () -> {
            uuidv5.open(null, udfContext);
        });

    }

    @Test
    public void testUUID() {
        udfContext = new UDFContext();
        UUID uuid = new UUID();
        parameters =  new HashMap<>();
        udfContext.setParameters(parameters);
        udfContext.setOutputFields(Collections.singletonList("uuid"));
        uuid.open(null, udfContext);
        Event event = new Event();
        Map<String, Object> extractedFields = new HashMap<>();
        event.setExtractedFields(extractedFields);
        Event result1 = uuid.evaluate(event);
        assertEquals(36, result1.getExtractedFields().get("uuid").toString().length());
    }
    @Test
    public void testUUIDV7() {
        udfContext = new UDFContext();
        UUIDv7 uuid = new UUIDv7();
        parameters =  new HashMap<>();
        udfContext.setParameters(parameters);
        udfContext.setOutputFields(Collections.singletonList("uuid"));
        uuid.open(null, udfContext);
        Event event = new Event();
        Map<String, Object> extractedFields = new HashMap<>();
        event.setExtractedFields(extractedFields);
        Event result1 = uuid.evaluate(event);
        assertEquals(36, result1.getExtractedFields().get("uuid").toString().length());
    }
    @Test
    public void testUUIDV5ForNameSpaceIp() {
        udfContext = new UDFContext();
        UUIDv5 uuidv5 = new UUIDv5();
        parameters =  new HashMap<>();
        udfContext.setParameters(parameters);
        udfContext.setLookupFields(List.of("client_ip", "server_ip"));
        udfContext.setOutputFields(Collections.singletonList("uuid"));
        parameters.put("namespace","NAMESPACE_IP");
        uuidv5.open(null, udfContext);
        Event event = new Event();
        Map<String, Object> extractedFields = new HashMap<>();
        extractedFields.put("client_ip", "1.1.1.1");
        extractedFields.put("server_ip", "");
        event.setExtractedFields(extractedFields);
        Event result = uuidv5.evaluate(event);
        System.out.printf("uuid: %s\n", result.getExtractedFields().get("uuid").toString());
        assertEquals("5394a6a8-b9b8-5147-b5b2-01365f158acb", result.getExtractedFields().get("uuid").toString());
        assertNotEquals("ecc67867-1f76-580c-a4c1-6a3d16ad6d02", result.getExtractedFields().get("uuid").toString());
    }

    @Test
    public void testUUIDV5ForNameSpaceDomain() {
        udfContext = new UDFContext();
        UUIDv5 uuidv5 = new UUIDv5();
        parameters =  new HashMap<>();
        udfContext.setParameters(parameters);
        udfContext.setLookupFields(List.of("domain"));
        udfContext.setOutputFields(Collections.singletonList("uuid"));
        parameters.put("namespace","NAMESPACE_DOMAIN");
        uuidv5.open(null, udfContext);
        Event event = new Event();
        Map<String, Object> extractedFields = new HashMap<>();
        extractedFields.put("domain", "baidu");
        event.setExtractedFields(extractedFields);
        Event result1 = uuidv5.evaluate(event);
        assertEquals("fd67cec1-6b33-5def-835c-fbe32f1ce4a4", result1.getExtractedFields().get("uuid").toString());
    }
    @Test
    public void testUUIDv5ForNameSpaceApp() {
        udfContext = new UDFContext();
        UUIDv5 uuidv5 = new UUIDv5();
        parameters =  new HashMap<>();
        udfContext.setParameters(parameters);
        udfContext.setLookupFields(List.of("app"));
        udfContext.setOutputFields(Collections.singletonList("uuid"));
        parameters.put("namespace","NAMESPACE_APP");
        uuidv5.open(null, udfContext);
        Event event = new Event();
        Map<String, Object> extractedFields = new HashMap<>();
        extractedFields.put("app", "54321");
        event.setExtractedFields(extractedFields);
        Event result1 = uuidv5.evaluate(event);
        assertEquals("194b8304-64f0-599e-bf7a-66cbc21bf6ee", result1.getExtractedFields().get("uuid").toString());
    }

    @Test
    public void testUUIDV5ForNameSpaceSubscriberID() {
        udfContext = new UDFContext();
        UUIDv5 uuidv5 = new UUIDv5();
        parameters =  new HashMap<>();
        udfContext.setParameters(parameters);
        udfContext.setLookupFields(List.of("subscriber_id"));
        udfContext.setOutputFields(Collections.singletonList("uuid"));
        parameters.put("namespace","NAMESPACE_SUBSCRIBER");
        uuidv5.open(null, udfContext);
        Event event = new Event();
        Map<String, Object> extractedFields = new HashMap<>();
        extractedFields.put("subscriber_id", "test1");
        event.setExtractedFields(extractedFields);
        Event result1 = uuidv5.evaluate(event);
        assertEquals("9b154520-3c29-541c-bb81-f649354dae67", result1.getExtractedFields().get("uuid").toString());
    }
}
