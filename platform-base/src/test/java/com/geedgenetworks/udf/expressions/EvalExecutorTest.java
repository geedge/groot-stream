package com.geedgenetworks.udf.expressions;

import com.geedgenetworks.expressions.EvalExecutor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

// Tests for  EvalExecutor
public class EvalExecutorTest {

    private static Map<String, Object> origin;

    private static EvalExecutor executor;

    @BeforeAll
    static void setUp() {
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

        executor = new EvalExecutor(origin);
    }

    @Test
    void testValueExpression() {
        // constant
        assertEquals(executor.eval("'name'"), "name");
        assertEquals(executor.eval("5"), 5);
        assertEquals(executor.eval("5L"), 5L);
        assertEquals(executor.eval("5D"), (double) 5L);
        assertEquals(executor.eval("5BD"), BigDecimal.valueOf(5));
        assertEquals(executor.eval("TRUE"), true);
        assertEquals(executor.eval("FALSE"), false);

        // variable
        assertEquals(executor.eval("common_client_ip"), origin.get("common_client_ip"));
        assertEquals(executor.eval("common_client_port"), origin.get("common_client_port"));
    }

    @Test
    void testConditionExpression() {
        int commonClientPort = (int) origin.get("common_client_port");
        int commonServerPort = (int) origin.get("common_server_port");
        int commonC2sPktNum = (int) origin.get("common_c2s_pkt_num");
        int commonS2cPktNum = (int) origin.get("common_s2c_pkt_num");
        int commonC2sByteNum = (int) origin.get("common_c2s_byte_num");
        int commonS2cByteNum = (int) origin.get("common_s2c_byte_num");
        assertEquals(
                executor.eval("common_client_port > common_server_port ? common_c2s_pkt_num : common_s2c_pkt_num"),
                commonClientPort > commonServerPort ? commonC2sPktNum : commonS2cPktNum);
        assertEquals(
                executor.eval("common_client_port > common_server_port " +
                        "? 'C2S:' + common_c2s_pkt_num : 'S2C:' + common_s2c_pkt_num"),
                commonClientPort > commonServerPort ? "C2S:" + commonC2sPktNum : "S2C:" + commonS2cPktNum);
        assertEquals(
                executor.eval("common_c2s_pkt_num > common_s2c_pkt_num " +
                        "& common_c2s_byte_num > common_s2c_byte_num ? common_client_port : common_server_port"),
                commonC2sPktNum > commonS2cPktNum
                        && commonC2sByteNum > commonS2cByteNum ? commonClientPort : commonServerPort
        );
    }

    @Test
    void testSyntaxError() {
        assertThrows(EvalExecutor.SyntaxErrorException.class,
                () -> executor.eval("common_c2s_pkt_num > common_s2c_pkt_num"));
        assertThrows(EvalExecutor.SyntaxErrorException.class,
                () -> executor.eval("2 = 3"));
        assertThrows(EvalExecutor.SyntaxErrorException.class,
                () -> executor.eval("common_client_ip - 10"));
    }
}