package com.geedgenetworks.core.connector.inline;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.*;

class InlineTableFactoryTest {

    @Test
    public void testDataDecode(){
        InlineTableFactory factory = new InlineTableFactory();
        String str = "utf8字符串";
        String data = "utf8字符串";
        assertArrayEquals(factory.getDataBytes(data, "string"), str.getBytes(StandardCharsets.UTF_8));

        data = "75746638e5ad97e7aca6e4b8b2";
        assertArrayEquals(factory.getDataBytes(data, "hex"), str.getBytes(StandardCharsets.UTF_8));
        data = "75746638E5AD97E7ACA6E4B8B2";
        assertArrayEquals(factory.getDataBytes(data, "hex"), str.getBytes(StandardCharsets.UTF_8));

        data = Base64.getEncoder().encodeToString(str.getBytes(StandardCharsets.UTF_8));
        assertArrayEquals(factory.getDataBytes(data, "base64"), str.getBytes(StandardCharsets.UTF_8));
    }

}