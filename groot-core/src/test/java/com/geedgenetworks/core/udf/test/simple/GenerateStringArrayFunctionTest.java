package com.geedgenetworks.core.udf.test.simple;

import com.geedgenetworks.core.udf.GenerateStringArray;
import com.geedgenetworks.spi.common.udf.UDFContext;
import com.geedgenetworks.spi.table.event.Event;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class GenerateStringArrayFunctionTest {
    private static UDFContext udfContext;

    @BeforeAll
    public static void setUp() {
        udfContext = new UDFContext();
        udfContext.setLookupFields(Arrays.asList("t1", "t2","t3","t4","t5"));
        udfContext.setOutputFields(Collections.singletonList("result_list"));
    }

    // 测试方法

    @Test
    public void testGenerateStringArrayFunction() {
        GenerateStringArray generateStringArray = new GenerateStringArray();
        generateStringArray.open(null, udfContext);
        Event event = new Event();
        Map<String, Object> extractedFields = new HashMap<>();
        extractedFields.put("t1", "6975");
        extractedFields.put("t2", 333);
        extractedFields.put("t3", Arrays.asList("1", "2"));
        String[] array = {"1", "2"};
        extractedFields.put("t4", array);
        extractedFields.put("t5", null);
        event.setExtractedFields(extractedFields);
        Event result = generateStringArray.evaluate(event);
        List<String> resultList = (List<String>) result.getExtractedFields().get("result_list");
        assertEquals("6975", resultList.get(0));
        assertEquals("333", resultList.get(1));
        assertEquals("1", resultList.get(2));
        assertEquals("2", resultList.get(3));
        assertEquals("1", resultList.get(4));
        assertEquals("2", resultList.get(5));
        assertEquals(6, resultList.size());



    }
}
