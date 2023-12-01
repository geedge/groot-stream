package com.geedgenetworks.core.udf.test;

import com.geedgenetworks.core.expressions.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;

class EvalExpressionResolverTest {

    private static EvalExpressionResolver resolver;

    @BeforeAll
    static void setUp() {
        resolver = new EvalExpressionResolver();
    }

    @Test
    void testValueExpression() {
        // constant
        assertEquals(resolver.resolve("'name'"), new ValueExpression.StringConstantExpression("name"));
        assertEquals(resolver.resolve("5"), new ValueExpression.NumberConstantExpression(5));
        assertEquals(resolver.resolve("5L"), new ValueExpression.NumberConstantExpression(5L));
        assertEquals(resolver.resolve("5D"), new ValueExpression.NumberConstantExpression((double) 5L));
        assertEquals(resolver.resolve("5BD"), new ValueExpression.NumberConstantExpression(BigDecimal.valueOf(5)));
        assertEquals(resolver.resolve("TRUE"), new ValueExpression.BooleanConstantExpression(true));
        assertEquals(resolver.resolve("FALSE"), new ValueExpression.BooleanConstantExpression(false));

        // variable
        assertEquals(resolver.resolve("common_client_ip"),
                new ValueExpression.FieldRefExpression("common_client_ip"));
        assertEquals(resolver.resolve("common_client_port"),
                new ValueExpression.FieldRefExpression("common_client_port"));
    }

    @Test
    void testConditionExpression() {

        assertEquals(
                resolver.resolve("common_client_port > common_server_port ? common_c2s_pkt_num : common_s2c_pkt_num"),
                new ValueExpression.TernaryExpression(
                        new BooleanExpression.ComparisonExpression(ComparisonOperator.GT,
                                new ValueExpression.FieldRefExpression("common_client_port"),
                                new ValueExpression.FieldRefExpression("common_server_port")),
                        new ValueExpression.FieldRefExpression("common_c2s_pkt_num"),
                        new ValueExpression.FieldRefExpression("common_s2c_pkt_num"))
        );
        assertEquals(
                resolver.resolve("common_client_port > common_server_port " +
                        "? 'C2S:' + common_c2s_pkt_num : 'S2C:' + common_s2c_pkt_num"),
                new ValueExpression.TernaryExpression(
                        new BooleanExpression.ComparisonExpression(ComparisonOperator.GT,
                                new ValueExpression.FieldRefExpression("common_client_port"),
                                new ValueExpression.FieldRefExpression("common_server_port")),
                        new ValueExpression.ArithmeticExpression(ArithmeticOperator.PLUS,
                                new ValueExpression.StringConstantExpression("C2S:"),
                                new ValueExpression.FieldRefExpression("common_c2s_pkt_num")),
                        new ValueExpression.ArithmeticExpression(ArithmeticOperator.PLUS,
                                new ValueExpression.StringConstantExpression("S2C:"),
                                new ValueExpression.FieldRefExpression("common_s2c_pkt_num")))
                );
        assertEquals(
                resolver.resolve("common_c2s_pkt_num > common_s2c_pkt_num " +
                        "& common_c2s_byte_num > common_s2c_byte_num ? common_client_port : common_server_port"),
                new ValueExpression.TernaryExpression(
                        new BooleanExpression.PredicateExpression(PredicateOperator.AND,
                                new BooleanExpression.ComparisonExpression(ComparisonOperator.GT,
                                        new ValueExpression.FieldRefExpression("common_c2s_pkt_num"),
                                        new ValueExpression.FieldRefExpression("common_s2c_pkt_num")),
                                new BooleanExpression.ComparisonExpression(ComparisonOperator.GT,
                                        new ValueExpression.FieldRefExpression("common_c2s_byte_num"),
                                        new ValueExpression.FieldRefExpression("common_s2c_byte_num"))),
                        new ValueExpression.FieldRefExpression("common_client_port"),
                        new ValueExpression.FieldRefExpression("common_server_port"))
        );
    }


}