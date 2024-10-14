package com.geedgenetworks.core.udf.test.simple;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.common.udf.UDFContext;
import com.geedgenetworks.core.udf.uuid.Uuid;
import com.geedgenetworks.core.udf.uuid.UuidV5;
import com.geedgenetworks.core.udf.uuid.UuidV7;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class UuidFunctionTest {

    private UDFContext udfContext;
    private Map<String, Object> parameters ;


    @Test
    public void testInit(){
        udfContext = new UDFContext();
        UuidV5 uuidv5 = new UuidV5();
        parameters =  new HashMap<>();
        udfContext.setParameters(parameters);
        udfContext.setLookup_fields(List.of("client_ip","server_ip"));
        udfContext.setOutput_fields(Collections.singletonList("uuid"));
        parameters.put("namespace","NAMESPACE_IP_1");
        Assertions.assertThrows(GrootStreamRuntimeException.class, () -> {
            uuidv5.open(null, udfContext);
        });

    }

    @Test
    public void testUuid() {
        udfContext = new UDFContext();
        Uuid uuid = new Uuid();
        parameters =  new HashMap<>();
        udfContext.setParameters(parameters);
        udfContext.setOutput_fields(Collections.singletonList("uuid"));
        uuid.open(null, udfContext);
        Event event = new Event();
        Map<String, Object> extractedFields = new HashMap<>();
        event.setExtractedFields(extractedFields);
        Event result1 = uuid.evaluate(event);
        assertEquals(36, result1.getExtractedFields().get("uuid").toString().length());
    }
    @Test
    public void testUuidV7() {
        udfContext = new UDFContext();
        UuidV7 uuid = new UuidV7();
        parameters =  new HashMap<>();
        udfContext.setParameters(parameters);
        udfContext.setOutput_fields(Collections.singletonList("uuid"));
        uuid.open(null, udfContext);
        Event event = new Event();
        Map<String, Object> extractedFields = new HashMap<>();
        event.setExtractedFields(extractedFields);
        Event result1 = uuid.evaluate(event);
        assertEquals(36, result1.getExtractedFields().get("uuid").toString().length());
    }
    @Test
    public void testUuidV5ForNameSpaceIp() {
        udfContext = new UDFContext();
        UuidV5 uuidv5 = new UuidV5();
        parameters =  new HashMap<>();
        udfContext.setParameters(parameters);
        udfContext.setLookup_fields(List.of("client_ip","server_ip"));
        udfContext.setOutput_fields(Collections.singletonList("uuid"));
        parameters.put("namespace","NAMESPACE_IP");
        uuidv5.open(null, udfContext);
        Event event = new Event();
        Map<String, Object> extractedFields = new HashMap<>();
        extractedFields.put("client_ip", "1.1.1.1");
        extractedFields.put("server_ip", "1.1.1.2");
        event.setExtractedFields(extractedFields);
        Event result1 = uuidv5.evaluate(event);
        assertEquals("52530d0c-07df-5c4b-a659-661242575386", result1.getExtractedFields().get("uuid").toString());
    }

    @Test
    public void testUuidV5ForNameSpaceDomain() {
        udfContext = new UDFContext();
        UuidV5 uuidv5 = new UuidV5();
        parameters =  new HashMap<>();
        udfContext.setParameters(parameters);
        udfContext.setLookup_fields(List.of("domain"));
        udfContext.setOutput_fields(Collections.singletonList("uuid"));
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
    public void testUuidV5ForNameSpaceApp() {
        udfContext = new UDFContext();
        UuidV5 uuidv5 = new UuidV5();
        parameters =  new HashMap<>();
        udfContext.setParameters(parameters);
        udfContext.setLookup_fields(List.of("app"));
        udfContext.setOutput_fields(Collections.singletonList("uuid"));
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
    public void testUuidV5ForNameSpaceSubid() {
        udfContext = new UDFContext();
        UuidV5 uuidv5 = new UuidV5();
        parameters =  new HashMap<>();
        udfContext.setParameters(parameters);
        udfContext.setLookup_fields(List.of("subid"));
        udfContext.setOutput_fields(Collections.singletonList("uuid"));
        parameters.put("namespace","NAMESPACE_SUBSCRIBER");
        uuidv5.open(null, udfContext);
        Event event = new Event();
        Map<String, Object> extractedFields = new HashMap<>();
        extractedFields.put("subid", "test1");
        event.setExtractedFields(extractedFields);
        Event result1 = uuidv5.evaluate(event);
        assertEquals("9b154520-3c29-541c-bb81-f649354dae67", result1.getExtractedFields().get("uuid").toString());
    }
}
