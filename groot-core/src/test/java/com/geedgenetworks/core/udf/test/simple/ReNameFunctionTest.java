package com.geedgenetworks.core.udf.test.simple;

import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;
import com.geedgenetworks.core.udf.Rename;
import com.geedgenetworks.core.utils.ASNKnowledgeBase;
import com.geedgenetworks.core.utils.KnowledgeBaseSchedule;
import com.geedgenetworks.utils.IpLookupV2;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ReNameFunctionTest {
    private static UDFContext udfContext;

    @BeforeAll
    public static void setUp() {
        udfContext = new UDFContext();
        udfContext.setLookup_fields(Arrays.asList("oldName"));
        udfContext.setOutput_fields(Arrays.asList("newName"));

    }

    // 测试方法

    @Test
    public void testRenameFunction() {
        Rename rename = new Rename();
        rename.open(null, udfContext);
        Event event = new Event();
        Map<String, Object> extractedFields = new HashMap<>();
        extractedFields.put("oldName", "old");
        event.setExtractedFields(extractedFields);
        Event result1 = rename.evaluate(event);
        assertEquals("old", result1.getExtractedFields().get("newName"));
    }
}
