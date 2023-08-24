package com.geedgenetworks.udf.expressions;

import com.geedgenetworks.udf.parser.EvalExpressionBaseVisitor;
import com.geedgenetworks.udf.parser.EvalExpressionLexer;
import com.geedgenetworks.udf.parser.EvalExpressionParser;
import com.google.common.base.Strings;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Objects;

/**
 * This class represents an evaluator for expressions using a visitor pattern.
 * It can evaluate arithmetic, logical, and comparison expressions, as well as handle variables and literals.
 *
 * @author chaoc
 * @version 1.0
 */
public class EvalExecutor extends EvalExpressionBaseVisitor<Object> {

    // The original variable values for expression evaluation.
    private final Map<String, Object> origin;

    /**
     * Constructs an EvalExecutor with the specified variable values.
     *
     * @param origin The map of variable names and their corresponding values.
     */
    public EvalExecutor(final Map<String, Object> origin) {
        this.origin = origin;
    }

    /**
     * Evaluates the given expression and returns the result as an object.
     *
     * @param expr The expression to be evaluated.
     * @return The result of the evaluation.
     */
    public Object eval(final String expr) {
        EvalExpressionLexer lexer = new EvalExpressionLexer(
                CharStreams.fromString(expr)
        );
        lexer.removeErrorListeners();

        EvalExpressionParser parser = new EvalExpressionParser(new CommonTokenStream(lexer));
        parser.getInterpreter().setPredictionMode(PredictionMode.LL);
        parser.setErrorHandler(new DefaultErrorStrategy() {

            @Override
            protected void reportInputMismatch(Parser recognizer, InputMismatchException e) {
                Token currentToken = recognizer.getCurrentToken();
                String message = "Miss matched input: ";
                message += this.getTokenErrorDisplay(e.getOffendingToken());
                message += ";\n";
                message += "Expect: " + e.getExpectedTokens().toString(recognizer.getVocabulary());
                message += "\n";
                message += StringUtils.split(expr, "\n")[currentToken.getLine() - 1];
                message += "\n";
                message += Strings.repeat(" ", currentToken.getTokenIndex()) + '^';
                throw new SyntaxErrorException(message);
            }
        });
        return this.visit(parser.statement());
    }

    /**
     * Evaluates the given expression and returns the result as an object.
     *
     * @param expr   The expression to be evaluated.
     * @param origin The map of variable names and their corresponding values.
     * @return The result of the evaluation.
     */
    public static Object eval(final String expr, final Map<String, Object> origin) {
        return new EvalExecutor(origin).eval(expr);
    }

    @Override
    public Object visitAssignValue(EvalExpressionParser.AssignValueContext ctx) {
        return visitValueExpression(ctx.valueExpression());
    }

    @Override
    public Object visitPredicated(EvalExpressionParser.PredicatedContext ctx) {
        if (visitBooleanExpression(ctx.booleanExpression())) {
            return visitValueExpression(ctx.trueValue);
        } else {
            return visitValueExpression(ctx.falseValue);
        }
    }

    @Override
    public Boolean visitValueComparison(EvalExpressionParser.ValueComparisonContext ctx) {
        Object left = visitValueExpression(ctx.left);
        Object right = visitValueExpression(ctx.right);

        ComparisonOperator operator = visitComparisonOperator(ctx.operator);
        switch (operator) {
            case EQ:
                return Objects.equals(left, right);
            case NEQ:
                return !Objects.equals(left, right);
            default:
                if (left instanceof Number && right instanceof Number) {
                    return compare((Number) left, (Number) right, operator);
                }
        }
        return false;
    }

    @Override
    public Boolean visitLogicalComparison(EvalExpressionParser.LogicalComparisonContext ctx) {
        Boolean left = visitBooleanExpression(ctx.left);
        Boolean right = visitBooleanExpression(ctx.right);
        PredicateOperator predicateOperator = visitPredicateOperator(ctx.operator);
        switch (predicateOperator) {
            case AND:
                return left && right;
            case OR:
                return left || right;
        }
        return false;
    }

    @Override
    public Boolean visitNotComparison(EvalExpressionParser.NotComparisonContext ctx) {
        return !visitBooleanExpression(ctx);
    }

    @Override
    public Object visitVariable(EvalExpressionParser.VariableContext ctx) {
        String key = visitIdentifier(ctx.identifier());
        return origin.get(key);
    }

    @Override
    public String visitIdentifier(EvalExpressionParser.IdentifierContext ctx) {
        if (ctx.IDENTIFIER() != null) {
            return ctx.IDENTIFIER().getText();
        }
        return null;
    }

    @Override
    public Object visitArithmeticValue(EvalExpressionParser.ArithmeticValueContext ctx) {
        Object left = visitValueExpression(ctx.left);
        Object right = visitValueExpression(ctx.right);
        ArithmeticOperator operator = visitArithmeticOperator(ctx.operator);
        if (left instanceof Number && right instanceof Number) {
            return this.compute((Number) left, (Number) right, operator);
        }
        if ((left instanceof String || right instanceof String) && ArithmeticOperator.PLUS.equals(operator)) {
            return left.toString() + right.toString();
        }
        throw new SyntaxErrorException(String.format("Error: %s, unable to calculate: %s [%s] %s %s [%s]",
                ctx.getText(),
                left, left.getClass().getSimpleName(),
                operator.getValue(),
                right, right.getClass().getSimpleName()));
    }

    private Object visitValueExpression(EvalExpressionParser.ValueExpressionContext ctx) {
        if (ctx instanceof EvalExpressionParser.ConstantValueContext) {
            return visitConstantValue((EvalExpressionParser.ConstantValueContext) ctx);
        } else if (ctx instanceof EvalExpressionParser.VariableContext) {
            return visitVariable((EvalExpressionParser.VariableContext) ctx);
        } else {
            return visitArithmeticValue((EvalExpressionParser.ArithmeticValueContext) ctx);
        }
    }

    @Override
    public Object visitConstantValue(EvalExpressionParser.ConstantValueContext ctx) {
        EvalExpressionParser.ConstantContext context = ctx.constant();
        if (context instanceof EvalExpressionParser.NullLiteralContext) {
            return visitNullLiteral((EvalExpressionParser.NullLiteralContext) context);
        } else if (context instanceof EvalExpressionParser.NumericLiteralContext) {
            return visitNumericLiteral((EvalExpressionParser.NumericLiteralContext) context);
        } else if (context instanceof EvalExpressionParser.BooleanLiteralContext) {
            return visitBooleanLiteral((EvalExpressionParser.BooleanLiteralContext) context);
        } else {
            return visitStringLiteral((EvalExpressionParser.StringLiteralContext) context);
        }
    }

    @Override
    public Object visitNullLiteral(EvalExpressionParser.NullLiteralContext ctx) {
        return null;
    }

    @Override
    public Number visitNumericLiteral(EvalExpressionParser.NumericLiteralContext ctx) {
        EvalExpressionParser.NumberContext context = ctx.number();
        if (context instanceof EvalExpressionParser.IntegerLiteralContext) {
            return visitIntegerLiteral((EvalExpressionParser.IntegerLiteralContext) context);
        } else if (context instanceof EvalExpressionParser.BigIntLiteralContext) {
            return visitBigIntLiteral((EvalExpressionParser.BigIntLiteralContext) context);
        } else if (context instanceof EvalExpressionParser.SmallIntLiteralContext) {
            return visitSmallIntLiteral((EvalExpressionParser.SmallIntLiteralContext) context);
        } else if (context instanceof EvalExpressionParser.DoubleLiteralContext) {
            return visitDoubleLiteral((EvalExpressionParser.DoubleLiteralContext) context);
        } else if (context instanceof EvalExpressionParser.FloatLiteralContext) {
            return visitFloatLiteral((EvalExpressionParser.FloatLiteralContext) context);
        } else {
            return visitBigDecimalLiteral((EvalExpressionParser.BigDecimalLiteralContext) context);
        }
    }

    @Override
    public Boolean visitBooleanLiteral(EvalExpressionParser.BooleanLiteralContext ctx) {
        return ctx.TRUE() != null;
    }

    @Override
    public String visitStringLiteral(EvalExpressionParser.StringLiteralContext ctx) {
        String text = ctx.STRING().getText();
        return StringUtils.replacePattern(text, "[\"']", "");
    }

    @Override
    public Integer visitIntegerLiteral(EvalExpressionParser.IntegerLiteralContext ctx) {
        int factor = ctx.MINUS() != null ? -1 : 1;
        return factor * Integer.parseInt(ctx.INTEGER_VALUE().getText());
    }

    @Override
    public Long visitBigIntLiteral(EvalExpressionParser.BigIntLiteralContext ctx) {
        int factor = ctx.MINUS() != null ? -1 : 1;
        return factor * Long.parseLong(ctx.BIGINT_LITERAL().getText()
                .replace("L", ""));
    }

    @Override
    public Short visitSmallIntLiteral(EvalExpressionParser.SmallIntLiteralContext ctx) {
        int factor = ctx.MINUS() != null ? -1 : 1;
        return (short) (factor * Short.parseShort(ctx.SMALLINT_LITERAL().getText()
                .replace("S", "")));
    }

    @Override
    public Double visitDoubleLiteral(EvalExpressionParser.DoubleLiteralContext ctx) {
        int factor = ctx.MINUS() != null ? -1 : 1;
        return factor * Double.parseDouble(ctx.DOUBLE_LITERAL().getText()
                .replace("D", ""));
    }

    @Override
    public Float visitFloatLiteral(EvalExpressionParser.FloatLiteralContext ctx) {
        int factor = ctx.MINUS() != null ? -1 : 1;
        return factor * Float.parseFloat(ctx.FLOAT_LITERAL().getText()
                .replace("F", ""));
    }

    @Override
    public BigDecimal visitBigDecimalLiteral(EvalExpressionParser.BigDecimalLiteralContext ctx) {
        BigDecimal value = new BigDecimal(ctx.BIGDECIMAL_LITERAL().getText().replace("BD", ""));
        return ctx.MINUS() == null ? value : value.negate();
    }

    @Override
    public ComparisonOperator visitComparisonOperator(EvalExpressionParser.ComparisonOperatorContext ctx) {
        if (ctx.EQ() != null) {
            return ComparisonOperator.EQ;
        } else if (ctx.NEQ() != null) {
            return ComparisonOperator.NEQ;
        } else if (ctx.LT() != null) {
            return ComparisonOperator.LT;
        } else if (ctx.LTE() != null) {
            return ComparisonOperator.LTE;
        } else if (ctx.GT() != null) {
            return ComparisonOperator.GT;
        } else {
            return ComparisonOperator.GTE;
        }
    }

    @Override
    public ArithmeticOperator visitArithmeticOperator(EvalExpressionParser.ArithmeticOperatorContext ctx) {
        if (ctx.PLUS() != null) {
            return ArithmeticOperator.PLUS;
        } else if (ctx.MINUS() != null) {
            return ArithmeticOperator.MINUS;
        } else if (ctx.ASTERISK() != null) {
            return ArithmeticOperator.ASTERISK;
        } else if (ctx.SLASH() != null) {
            return ArithmeticOperator.ASTERISK;
        } else {
            return ArithmeticOperator.PERCENT;
        }
    }

    @Override
    public PredicateOperator visitPredicateOperator(EvalExpressionParser.PredicateOperatorContext ctx) {
        if (ctx.AND() != null) {
            return PredicateOperator.AND;
        }
        return PredicateOperator.OR;
    }

    /**
     * Exception class representing a syntax error that may occur during program execution.
     */
    public static class SyntaxErrorException extends RuntimeException {

        public SyntaxErrorException() {
        }

        public SyntaxErrorException(String message) {
            super(message);
        }
    }

    // ======================================================================================
    // ----------------------------------- private helper -----------------------------------

    private Boolean visitBooleanExpression(EvalExpressionParser.BooleanExpressionContext ctx) {
        if (ctx instanceof EvalExpressionParser.LogicalComparisonContext) {
            EvalExpressionParser.LogicalComparisonContext context = (EvalExpressionParser.LogicalComparisonContext) ctx;
            EvalExpressionParser.BooleanExpressionContext left = context.left;
            EvalExpressionParser.BooleanExpressionContext right = context.right;
            EvalExpressionParser.PredicateOperatorContext operator = context.operator;
            if (operator.AND() != null) {
                return visitBooleanExpression(left) && visitBooleanExpression(right);
            } else {
                return visitBooleanExpression(left) || visitBooleanExpression(right);
            }
        } else if (ctx instanceof EvalExpressionParser.ValueComparisonContext) {
            return visitValueComparison((EvalExpressionParser.ValueComparisonContext) ctx);
        } else {
            return visitNotComparison((EvalExpressionParser.NotComparisonContext) ctx);
        }
    }

    private boolean compare(Number left, Number right, ComparisonOperator operator) {
        double l = left.doubleValue();
        double r = right.doubleValue();
        switch (operator) {
            case GT:
                return l > r;
            case GTE:
                return l >= r;
            case LT:
                return l < r;
            case LTE:
                return l <= r;
        }
        return false;
    }

    /**
     * Enumeration representing comparison operators.
     */
    private enum ComparisonOperator {
        EQ, NEQ, LT, LTE, GT, GTE
    }

    /**
     * Enumeration representing predicate operators.
     */
    private enum PredicateOperator {
        OR, AND
    }

    private Number compute(final Number left, final Number right, final ArithmeticOperator operator) {
        if (left instanceof BigDecimal || right instanceof BigDecimal) {
            return BigDecimal.valueOf(computeDouble(left, right, operator));
        } else if (left instanceof Double || right instanceof Double) {
            return computeDouble(left, right, operator);
        } else if (left instanceof Long || right instanceof Long) {
            return computeDouble(left, right, operator).longValue();
        } else if (left instanceof Integer || right instanceof Integer) {
            return computeDouble(left, right, operator).intValue();
        } else if (left instanceof Float || right instanceof Float) {
            return computeDouble(left, right, operator).floatValue();
        } else {
            return computeDouble(left, right, operator).shortValue();
        }
    }

    private Double computeDouble(final Number left, final Number right, final ArithmeticOperator operator) {
        double l = left.doubleValue();
        double r = right.doubleValue();
        switch (operator) {
            case PLUS:
                return l + r;
            case MINUS:
                return l - r;
            case ASTERISK:
                return l * r;
            case SLASH:
                return l / r;
            default:
                return l % r;
        }
    }

    /**
     * Enumeration representing arithmetic operators.
     */
    private enum ArithmeticOperator {
        PLUS("+"), MINUS("-"),
        ASTERISK("*"), SLASH("\\"),
        PERCENT("%");

        private final String value;

        ArithmeticOperator(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }
}
