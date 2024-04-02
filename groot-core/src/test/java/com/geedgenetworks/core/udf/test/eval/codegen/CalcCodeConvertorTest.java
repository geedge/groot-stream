package com.geedgenetworks.core.udf.test.eval.codegen;


import com.geedgenetworks.core.expressions.InvalidProgramException;
import com.geedgenetworks.core.expressions.SyntaxErrorException;
import com.geedgenetworks.core.expressions.codegen.CalcCodeConvertor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CalcCodeConvertorTest {

    private static Map<String, Object> origin;

    @BeforeAll
    public static void setUp() {
        origin = new HashMap<>();
        origin.put("common_schema_type", "RTP");
        origin.put("common_sessions", 1);
        origin.put("common_stream_dir", 3);
        origin.put("common_vsys_id", 24);

        origin.put("common_server_ip", "172.17.200.50");
        origin.put("common_server_port", 36682);
        origin.put("common_s2c_byte_num", 7570);
        origin.put("common_s2c_pkt_num", 5);

        origin.put("common_client_ip", "172.17.201.16");
        origin.put("common_client_port", 42924);
        origin.put("common_c2s_byte_num", 1834);
        origin.put("common_c2s_pkt_num", 6);

        origin.put("common_out_dest_mac", "02:fc:08:dc:92:d7");
        origin.put("common_out_src_mac", "02:fc:08:dc:91:c3");
    }

    @Test
    public void testValueExpression() throws Exception {
        // constant
        assertEquals(CalcCodeConvertor.convert("'name'").eval(origin), "name");
        assertEquals(CalcCodeConvertor.convert("5").eval(origin), 5);
        assertEquals(CalcCodeConvertor.convert("5L").eval(origin), 5L);
        assertEquals(CalcCodeConvertor.convert("5D").eval(origin), (double) 5L);
        assertEquals(CalcCodeConvertor.convert("5BD").eval(origin), BigDecimal.valueOf(5));
        assertEquals(CalcCodeConvertor.convert("TRUE").eval(origin), true);
        assertEquals(CalcCodeConvertor.convert("FALSE").eval(origin), false);

        // variable
        assertEquals(CalcCodeConvertor.convert("common_client_ip").eval(origin),
                origin.get("common_client_ip"));
        assertEquals(CalcCodeConvertor.convert("common_client_port").eval(origin),
                origin.get("common_client_port"));
    }

    @Test
    public void testConditionExpression() throws Exception {
        int commonClientPort = (int) origin.get("common_client_port");
        int commonServerPort = (int) origin.get("common_server_port");
        int commonC2sPktNum = (int) origin.get("common_c2s_pkt_num");
        int commonS2cPktNum = (int) origin.get("common_s2c_pkt_num");
        int commonC2sByteNum = (int) origin.get("common_c2s_byte_num");
        int commonS2cByteNum = (int) origin.get("common_s2c_byte_num");
        assertEquals(
                CalcCodeConvertor.convert("common_client_port > common_server_port " +
                        "? common_c2s_pkt_num : common_s2c_pkt_num").eval(origin),
                commonClientPort > commonServerPort ? commonC2sPktNum : commonS2cPktNum);
        assertEquals(
                CalcCodeConvertor.convert("common_client_port > common_server_port " +
                        "? 'C2S:' + common_c2s_pkt_num : 'S2C:' + common_s2c_pkt_num").eval(origin),
                commonClientPort > commonServerPort ? "C2S:" + commonC2sPktNum : "S2C:" + commonS2cPktNum);
        assertEquals(
                CalcCodeConvertor.convert("common_c2s_pkt_num > common_s2c_pkt_num " +
                        "&& common_c2s_byte_num > common_s2c_byte_num ? common_client_port : common_server_port").eval(origin),
                commonC2sPktNum > commonS2cPktNum
                        && commonC2sByteNum > commonS2cByteNum ? commonClientPort : commonServerPort
        );
    }

    @Test
    public void testArithmeticExpression() throws Exception {
        final Map<String, Object> map = Collections.emptyMap();
        assertEquals(
                1, CalcCodeConvertor.convert("5 & 3").eval(map));
        assertEquals(
                7, CalcCodeConvertor.convert("5 | 3").eval(map));
        assertEquals(
                20, CalcCodeConvertor.convert("5 << 2").eval(map));
        assertEquals(
                1, CalcCodeConvertor.convert("5 >> 2").eval(map));
        assertEquals(
                8, CalcCodeConvertor.convert("5 + 3").eval(map));
        assertEquals(
                2, CalcCodeConvertor.convert("5 - 3").eval(map));
        assertEquals(
                15, CalcCodeConvertor.convert("5 * 3").eval(map));
        assertEquals(
                2, CalcCodeConvertor.convert("5 / 2").eval(map));
        assertEquals(
                2, CalcCodeConvertor.convert("5 % 3").eval(map));


        final Map<String, Object> map1 = new HashMap<>();
        map1.put("flags", 8);

        assertEquals("B",
                CalcCodeConvertor.convert(" flags & 8 == 9 ? 'A' : 'B'").eval(map1));
        assertEquals("A",
                CalcCodeConvertor.convert(" 5 | flags == 13 ? 'A' : 'B'").eval(map1));
    }

    @Test
    public void testError() {
        assertThrows(SyntaxErrorException.class,
                () -> CalcCodeConvertor.convert("common_c2s_pkt_num > common_s2c_pkt_num"));
        assertThrows(SyntaxErrorException.class,
                () -> CalcCodeConvertor.convert("2 = 3"));
        assertThrows(InvalidProgramException.class,
                () -> CalcCodeConvertor.convert("common_client_ip - 10"));
    }
}