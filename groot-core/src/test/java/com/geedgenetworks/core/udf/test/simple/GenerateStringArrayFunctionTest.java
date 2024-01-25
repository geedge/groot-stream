package com.geedgenetworks.core.udf.test.simple;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.udf.UDFContext;
import com.geedgenetworks.core.udf.GenerateStringArray;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class GenerateStringArrayFunctionTest {
    private static UDFContext udfContext;

    @BeforeAll
    public static void setUp() {
        udfContext = new UDFContext();
        udfContext.setLookup_fields(Arrays.asList("client_asn", "server_asn"));
        udfContext.setOutput_fields(Collections.singletonList("asn_list"));
    }

    // 测试方法

    @Test
    public void testGenerateStringArrayFunction() {
        GenerateStringArray generateStringArray = new GenerateStringArray();
        generateStringArray.open(null, udfContext);
        Event event = new Event();
        Map<String, Object> extractedFields = new HashMap<>();
        extractedFields.put("client_asn", "6975");
        extractedFields.put("server_asn", "333");
        event.setExtractedFields(extractedFields);
        Event result = generateStringArray.evaluate(event);
        List<String> resultList = (List<String>) result.getExtractedFields().get("asn_list");
        assertEquals("6975", resultList.get(0));
        assertEquals("333", resultList.get(1));

    }
}
