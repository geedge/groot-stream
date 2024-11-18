package com.geedgenetworks.core.expressions;

import com.geedgenetworks.api.expressions.AviatorExecutor;
import com.geedgenetworks.api.expressions.SyntaxErrorException;
import com.googlecode.aviator.exception.ExpressionRuntimeException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AviatorExecutorTest {

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
        assertEquals(new AviatorExecutor("'name'").eval(origin), "name");
        assertEquals(new AviatorExecutor("5").eval(origin), 5L);
        assertEquals(new AviatorExecutor("true").eval(origin), true);
        assertEquals(new AviatorExecutor("false").eval(origin), false);

        // variable
        assertEquals(new AviatorExecutor("common_client_ip").eval(origin),
                origin.get("common_client_ip"));
        assertEquals(new AviatorExecutor("common_client_port").eval(origin),
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
                new AviatorExecutor("common_client_port > common_server_port " +
                        "? common_c2s_pkt_num : common_s2c_pkt_num").eval(origin),
                commonClientPort > commonServerPort ? commonC2sPktNum : commonS2cPktNum);
        assertEquals(
                new AviatorExecutor("common_client_port > common_server_port " +
                        "? 'C2S:' + common_c2s_pkt_num : 'S2C:' + common_s2c_pkt_num").eval(origin),
                commonClientPort > commonServerPort ? "C2S:" + commonC2sPktNum : "S2C:" + commonS2cPktNum);
        assertEquals(
                new AviatorExecutor("common_c2s_pkt_num > common_s2c_pkt_num " +
                        "&& common_c2s_byte_num > common_s2c_byte_num ? common_client_port : common_server_port").eval(origin),
                commonC2sPktNum > commonS2cPktNum
                        && commonC2sByteNum > commonS2cByteNum ? commonClientPort : commonServerPort
        );

    }

    @Test
    public void testArithmeticExpression() throws Exception {
        final Map<String, Object> map = Collections.emptyMap();
        assertEquals(
                1L, new AviatorExecutor("5 & 3").eval(map));
        assertEquals(
                7L, new AviatorExecutor("5 | 3").eval(map));
        assertEquals(
                20L, new AviatorExecutor("5 << 2").eval(map));
        assertEquals(
                1L, new AviatorExecutor("5 >> 2").eval(map));
        assertEquals(
                8L, new AviatorExecutor("5 + 3").eval(map));
        assertEquals(
                2L, new AviatorExecutor("5 - 3").eval(map));
        assertEquals(
                15L, new AviatorExecutor("5 * 3").eval(map));
        assertEquals(
                2L, new AviatorExecutor("5 / 2").eval(map));
        assertEquals(
                2L, new AviatorExecutor("5 % 3").eval(map));


        final Map<String, Object> map1 = new HashMap<>();
        map1.put("flags", 8);

        assertEquals("B",
                new AviatorExecutor(" ( flags & 8 ) == 9 ? 'A' : 'B'").eval(map1));
        assertEquals("A",
                new AviatorExecutor(" ( 5 | flags ) == 13 ? 'A' : 'B'").eval(map1));
    }

    @Test
    public void testFieldArithmeticExpression() throws Exception {
        assertEquals(1L,
                new AviatorExecutor("common_c2s_pkt_num - common_s2c_pkt_num").eval(origin));

        assertEquals(17L, new AviatorExecutor("common_c2s_pkt_num + 10 + 1").eval(origin));

    }

    @Test
    public void testNullField() {
        var evalExecutor = new AviatorExecutor("( a > nil ? a : 0 ) + ( b > nil ? b : 10 )");
        assertEquals(10L, evalExecutor.eval(origin));
    }

    @Test
    public void testError() {
        assertThrows(SyntaxErrorException.class,
                () -> new AviatorExecutor("1L"));
        assertThrows(SyntaxErrorException.class,
                () -> new AviatorExecutor("2 = 3"));

        final Map<String, Object> map = new HashMap<>(origin);
        map.put("has_address", true);
        var convert = new AviatorExecutor("common_c2s_pkt_num + has_address");
        assertThrows(ExpressionRuntimeException.class, () -> convert.eval(map));
    }

}