package com.geedgenetworks.core.udf.test.simple;

import com.geedgenetworks.core.utils.SnowflakeIdUtils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SnowflakeIdFunctionTest {


    @Test
    public void testSnowflakeIdFunction() {
        SnowflakeIdUtils snowflakeId = new SnowflakeIdUtils(1L,1L);
        assertTrue((snowflakeId.nextId()>0));
    }
}
