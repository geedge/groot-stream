package com.geedgenetworks.core.udf.test.simple;

import com.geedgenetworks.common.Event;
import com.geedgenetworks.common.udf.UDFContext;
import com.geedgenetworks.core.udf.Rename;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ReNameFunctionTest {
    private static UDFContext udfContext;

    @BeforeAll
    public static void setUp() {
        udfContext = new UDFContext();
        udfContext.setLookup_fields(Collections.singletonList("oldName"));
        udfContext.setOutput_fields(Collections.singletonList("newName"));

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
