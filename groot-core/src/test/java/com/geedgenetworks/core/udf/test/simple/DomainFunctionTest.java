package com.geedgenetworks.core.udf.test.simple;

import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;
import com.geedgenetworks.core.udf.Domain;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DomainFunctionTest {

    private static UDFContext udfContext;

    private static Map<String, Object> parameters ;

    @BeforeAll
    public static void setUp() {
        udfContext = new UDFContext();
        parameters =  new HashMap<>();
        udfContext.setParameters(parameters);
        udfContext.setLookup_fields(Collections.singletonList("domain"));
        udfContext.setOutput_fields(Collections.singletonList("domain1"));
    }


    @Test
    public void testInit(){
        Domain domain = new Domain();
        udfContext.setLookup_fields(new ArrayList<>());
        udfContext.setParameters(new HashMap<>());
        udfContext.setParameters(null);
        Assertions.assertThrows(GrootStreamRuntimeException.class, () -> {
            domain.open(null, udfContext);
        });
        udfContext.setParameters(new HashMap<>());
        udfContext.getParameters().put("option","TOP_LEVEL_DOMAIN");
        Assertions.assertThrows(GrootStreamRuntimeException.class, () -> {
            domain.open(null, udfContext);
        });

        udfContext.setLookup_fields(new ArrayList<>());
        udfContext.getLookup_fields().add("v1");
        udfContext.setOutput_fields(new ArrayList<>());
        udfContext.getOutput_fields().add("v2");
        udfContext.setParameters(new HashMap<>());
        udfContext.getParameters().put("option","other");
        Assertions.assertThrows(GrootStreamRuntimeException.class, () -> {
            domain.open(null, udfContext);
        });

    }

    @Test
    public void testDomainFunctionTopLevelDomain() {
        parameters.put("option", "TOP_LEVEL_DOMAIN");
        udfContext.setLookup_fields(Collections.singletonList("domain"));
        udfContext.setOutput_fields(Collections.singletonList("domain1"));
        Domain domain = new Domain();
        domain.open(null, udfContext);
        Event event = new Event();
        Map<String, Object> extractedFields = new HashMap<>();
        extractedFields.put("domain", "www.baidu.com");
        event.setExtractedFields(extractedFields);
        Event result1 = domain.evaluate(event);
        assertEquals("com", result1.getExtractedFields().get("domain1"));
    }

    @Test
    public void testDomainFunctionFirstSignificantSubdomain() {

        parameters.put("option", "FIRST_SIGNIFICANT_SUBDOMAIN");
        udfContext.setParameters(parameters);
        udfContext.setLookup_fields(Collections.singletonList("domain"));
        udfContext.setOutput_fields(Collections.singletonList("domain1"));
        Domain domain = new Domain();
        domain.open(null, udfContext);
        Event event = new Event();
        Map<String, Object> extractedFields = new HashMap<>();
        extractedFields.put("domain", "www.baidu.com");
        event.setExtractedFields(extractedFields);
        Event result1 = domain.evaluate(event);
        assertEquals("baidu.com", result1.getExtractedFields().get("domain1"));
    }
}
