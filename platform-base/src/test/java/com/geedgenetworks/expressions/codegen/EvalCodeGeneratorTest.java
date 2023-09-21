package com.geedgenetworks.expressions.codegen;

import com.geedgenetworks.expressions.EvalExpressionResolver;
import com.geedgenetworks.expressions.Expression;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

class EvalCodeGeneratorTest {

    private static EvalExpressionResolver resolver;

    @BeforeAll
    static void setUp() {
        resolver = new EvalExpressionResolver();
    }

    @Test
    void testCodeGen() {
        Expression expression = resolver.resolve("common_client_port > common_server_port " +
                " ? 'C2S:' + common_c2s_pkt_num : 'S2C:' + common_s2c_pkt_num");
        CodeGeneratorContext context = new CodeGeneratorContext();
        expression.accept(new EvalCodeGenerator(context));
        String memberCode = context.reuseMemberCode();
        assertTrue(StringUtils.contains(memberCode, "C2S:"));
        assertTrue(StringUtils.contains(memberCode, "S2C:"));
    }

}
