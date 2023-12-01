package com.geedgenetworks.core.udf.test.simple;

import com.geedgenetworks.core.pojo.Event;
import com.geedgenetworks.core.pojo.UDFContext;
import com.geedgenetworks.core.udf.Rename;
import com.geedgenetworks.core.udf.SnowflakeId;
import com.geedgenetworks.core.utils.SnowflakeIdUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SnowflakeIdFunctionTest {


    @Test
    public void testSnowflakeIdFunction() {
        SnowflakeIdUtils snowflakeId = new SnowflakeIdUtils(1L,1L);
        assertTrue((snowflakeId.nextId()>0));
    }
}
