package com.geedgenetworks.core.udf.test.simple;

import com.geedgenetworks.core.udf.Drop;
import com.geedgenetworks.api.common.udf.UDFContext;
import com.geedgenetworks.api.event.Event;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class DropFunctionTest {

    private static UDFContext udfContext;

    @BeforeAll
    public static void setUp() {
        udfContext = new UDFContext();
        udfContext.setFilter("true");
        udfContext.setParameters(new HashMap<>());
    }

    // 测试方法

    @Test
    public void testDropFunctionTrue() {
        Drop drop = new Drop();
        drop.open(null, udfContext);
        Event event = new Event();
        Event result = drop.evaluate(event);
        assertTrue(result.isDropped());
    }



}
