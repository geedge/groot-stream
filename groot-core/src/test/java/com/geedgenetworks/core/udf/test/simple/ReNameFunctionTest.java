package com.geedgenetworks.core.udf.test.simple;

import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;
import com.geedgenetworks.core.udf.Rename;
import com.geedgenetworks.core.utils.ASNKnowledgeBase;
import com.geedgenetworks.core.utils.KnowledgeBaseFactory;
import com.geedgenetworks.utils.IpLookupV2;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
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
    @Test
    public void ww() throws Exception {
        KnowledgeBaseFactory.getInstance();
        KnowledgeBaseFactory.getKnowledgeBase(ASNKnowledgeBase.class);
        Map<String, IpLookupV2> ddd =ASNKnowledgeBase.venderWithAsnLookup;

        IpLookupV2 ffdf =ddd.get("asn_id");
        ffdf .asnLookup("120.233.20.242");
      System.out.println(ffdf .asnLookup("120.233.20.242"));
        ffdf .asnLookup("2600:1015:b002::");
        System.out.println(ffdf .asnLookup("2600:1015:b002::"));

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
