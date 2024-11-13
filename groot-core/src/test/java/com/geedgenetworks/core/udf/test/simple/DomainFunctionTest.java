package com.geedgenetworks.core.udf.test.simple;

import com.geedgenetworks.common.exception.GrootStreamRuntimeException;
import com.geedgenetworks.core.udf.Domain;
import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.event.Event;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
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
        udfContext.setLookupFields(Collections.singletonList("domain"));
        udfContext.setOutputFields(Collections.singletonList("domain1"));
    }


    @Test
    public void testInit(){
        Domain domain = new Domain();
        udfContext.setLookupFields(new ArrayList<>());
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

        udfContext.setLookupFields(new ArrayList<>());
        udfContext.getLookupFields().add("v1");
        udfContext.setOutputFields(new ArrayList<>());
        udfContext.getOutputFields().add("v2");
        udfContext.setParameters(new HashMap<>());
        udfContext.getParameters().put("option","other");
        Assertions.assertThrows(GrootStreamRuntimeException.class, () -> {
            domain.open(null, udfContext);
        });

    }

    @Test
    public void testDomainFunctionTopLevelDomain() {
        parameters.put("option", "TOP_LEVEL_DOMAIN");
        udfContext.setLookupFields(Collections.singletonList("domain"));
        udfContext.setOutputFields(Collections.singletonList("domain1"));
        Domain domain = new Domain();
        domain.open(null, udfContext);
        Event event = new Event();
        Map<String, Object> extractedFields = new HashMap<>();
        extractedFields.put("domain", "http://www.baidu.com.cn");
        event.setExtractedFields(extractedFields);
        Event result1 = domain.evaluate(event);
        assertEquals("com.cn", result1.getExtractedFields().get("domain1"));
    }

    @Test
    public void testDomainFunctionFirstSignificantSubdomain() {

        parameters.put("option", "FIRST_SIGNIFICANT_SUBDOMAIN");
        udfContext.setParameters(parameters);
        udfContext.setLookupFields(Collections.singletonList("domain"));
        udfContext.setOutputFields(Collections.singletonList("domain1"));
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
