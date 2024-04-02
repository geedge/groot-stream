package com.geedgenetworks.core.expressions;

import com.geedgenetworks.core.expressions.parser.EvalExpressionBaseVisitor;
import com.geedgenetworks.core.expressions.parser.EvalExpressionLexer;
import com.geedgenetworks.core.expressions.parser.EvalExpressionParser;
import com.google.common.base.Strings;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;

/**
 * This class represents an evaluator for expressions using a visitor pattern. It can evaluate
 * arithmetic, logical, and comparison expressions, as well as handle variables and literals.
 *
 * @author chaoc
 * @version 1.0
 */
@SuppressWarnings("Duplicates")
public class EvalExpressionResolver extends EvalExpressionBaseVisitor<Object> {

    public Expression resolve(final String expr) {
        EvalExpressionLexer lexer = new EvalExpressionLexer(CharStreams.fromString(expr));
        lexer.removeErrorListeners();

        EvalExpressionParser parser = new EvalExpressionParser(new CommonTokenStream(lexer));
        parser.getInterpreter().setPredictionMode(PredictionMode.LL);
        parser.setErrorHandler(
                new DefaultErrorStrategy() {

                    @Override
                    protected void reportInputMismatch(
                            Parser recognizer, InputMismatchException e) {
                        Token currentToken = recognizer.getCurrentToken();
                        String message = "Miss matched input: ";
                        message += this.getTokenErrorDisplay(e.getOffendingToken());
                        message += ";\n";
                        message +=
                                "Expect: "
                                        + e.getExpectedTokens()
                                        .toString(recognizer.getVocabulary());
                        message += "\n";
                        message += StringUtils.split(expr, "\n")[currentToken.getLine() - 1];
                        message += "\n";
                        message += Strings.repeat(" ", currentToken.getTokenIndex()) + '^';
                        throw new SyntaxErrorException(message);
                    }
                });
        return (Expression) this.visit(parser.statement());
    }

    @Override
    public Object visitAssignValue(EvalExpressionParser.AssignValueContext ctx) {
        return visitValueExpression(ctx.valueExpression());
    }

    @Override
    public ValueExpression visitPredicated(EvalExpressionParser.PredicatedContext ctx) {
        BooleanExpression booleanExpression = visitBooleanExpression(ctx.booleanExpression());
        ValueExpression trueValue = visitValueExpression(ctx.trueValue);
        ValueExpression falseValue = visitValueExpression(ctx.falseValue);
        return new ValueExpression.TernaryExpression(booleanExpression, trueValue, falseValue);
    }

    @Override
    public BooleanExpression visitValueComparison(EvalExpressionParser.ValueComparisonContext ctx) {
        ValueExpression left = visitValueExpression(ctx.left);
        ValueExpression right = visitValueExpression(ctx.right);
        ComparisonOperator operator = visitComparisonOperator(ctx.operator);
        return new BooleanExpression.ComparisonExpression(operator, left, right);
    }

    @Override
    public BooleanExpression visitLogicalComparison(
            EvalExpressionParser.LogicalComparisonContext ctx) {
        BooleanExpression left = visitBooleanExpression(ctx.left);
        BooleanExpression right = visitBooleanExpression(ctx.right);
        PredicateOperator operator = visitPredicateOperator(ctx.operator);
        return new BooleanExpression.PredicateExpression(operator, left, right);
    }

    @Override
    public BooleanExpression visitNotComparison(EvalExpressionParser.NotComparisonContext ctx) {
        return new BooleanExpression.NotExpression(visitBooleanExpression(ctx));
    }

    @Override
    public ValueExpression.FieldRefExpression visitVariable(
            EvalExpressionParser.VariableContext ctx) {
        String key = visitIdentifier(ctx.identifier());
        return new ValueExpression.FieldRefExpression(key);
    }

    @Override
    public String visitIdentifier(EvalExpressionParser.IdentifierContext ctx) {
        if (ctx.IDENTIFIER() != null) {
            return ctx.IDENTIFIER().getText();
        }
        return null;
    }

    @Override
    public ValueExpression.ArithmeticExpression visitArithmeticValue(
            EvalExpressionParser.ArithmeticValueContext ctx) {
        ValueExpression left = visitValueExpression(ctx.left);
        ValueExpression right = visitValueExpression(ctx.right);
        ArithmeticOperator operator = visitArithmeticOperator(ctx.operator);
        return new ValueExpression.ArithmeticExpression(operator, left, right);
    }

    @Override
    public ValueExpression.ConstantValueExpression<?> visitConstantValue(
            EvalExpressionParser.ConstantValueContext ctx) {
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
    public ValueExpression.StringConstantExpression visitNullLiteral(
            EvalExpressionParser.NullLiteralContext ctx) {
        return new ValueExpression.StringConstantExpression(null);
    }

    @Override
    public ValueExpression.NumberConstantExpression visitNumericLiteral(
            EvalExpressionParser.NumericLiteralContext ctx) {
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
    public ValueExpression.BooleanConstantExpression visitBooleanLiteral(
            EvalExpressionParser.BooleanLiteralContext ctx) {
        return new ValueExpression.BooleanConstantExpression(ctx.TRUE() != null);
    }

    @Override
    public ValueExpression.StringConstantExpression visitStringLiteral(
            EvalExpressionParser.StringLiteralContext ctx) {
        String text = ctx.STRING().getText();
        return new ValueExpression.StringConstantExpression(
                StringUtils.replacePattern(text, "[\"']", ""));
    }

    @Override
    public ValueExpression.NumberConstantExpression visitIntegerLiteral(
            EvalExpressionParser.IntegerLiteralContext ctx) {
        int factor = ctx.MINUS() != null ? -1 : 1;
        int value = factor * Integer.parseInt(ctx.INTEGER_VALUE().getText());
        return new ValueExpression.NumberConstantExpression(value);
    }

    @Override
    public ValueExpression.NumberConstantExpression visitBigIntLiteral(
            EvalExpressionParser.BigIntLiteralContext ctx) {
        int factor = ctx.MINUS() != null ? -1 : 1;
        long value = factor * Long.parseLong(ctx.BIGINT_LITERAL().getText().replace("L", ""));
        return new ValueExpression.NumberConstantExpression(value);
    }

    @Override
    public ValueExpression.NumberConstantExpression visitSmallIntLiteral(
            EvalExpressionParser.SmallIntLiteralContext ctx) {
        int factor = ctx.MINUS() != null ? -1 : 1;
        short value =
                (short)
                        (factor
                                * Short.parseShort(
                                ctx.SMALLINT_LITERAL().getText().replace("S", "")));
        return new ValueExpression.NumberConstantExpression(value);
    }

    @Override
    public ValueExpression.NumberConstantExpression visitDoubleLiteral(
            EvalExpressionParser.DoubleLiteralContext ctx) {
        int factor = ctx.MINUS() != null ? -1 : 1;
        double value = factor * Double.parseDouble(ctx.DOUBLE_LITERAL().getText().replace("D", ""));
        return new ValueExpression.NumberConstantExpression(value);
    }

    @Override
    public ValueExpression.NumberConstantExpression visitFloatLiteral(
            EvalExpressionParser.FloatLiteralContext ctx) {
        int factor = ctx.MINUS() != null ? -1 : 1;
        float value = factor * Float.parseFloat(ctx.FLOAT_LITERAL().getText().replace("F", ""));
        return new ValueExpression.NumberConstantExpression(value);
    }

    @Override
    public ValueExpression.NumberConstantExpression visitBigDecimalLiteral(
            EvalExpressionParser.BigDecimalLiteralContext ctx) {
        BigDecimal value = new BigDecimal(ctx.BIGDECIMAL_LITERAL().getText().replace("BD", ""));
        return new ValueExpression.NumberConstantExpression(
                ctx.MINUS() == null ? value : value.negate());
    }

    @Override
    public ComparisonOperator visitComparisonOperator(
            EvalExpressionParser.ComparisonOperatorContext ctx) {
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
    public ArithmeticOperator visitArithmeticOperator(
            EvalExpressionParser.ArithmeticOperatorContext ctx) {
        if (ctx.PLUS() != null) {
            return ArithmeticOperator.PLUS;
        } else if (ctx.MINUS() != null) {
            return ArithmeticOperator.MINUS;
        } else if (ctx.ASTERISK() != null) {
            return ArithmeticOperator.ASTERISK;
        } else if (ctx.SLASH() != null) {
            return ArithmeticOperator.SLASH;
        } else if (ctx.PERCENT() != null) {
            return ArithmeticOperator.PERCENT;
        } else if (ctx.LEFT_SHIFT() != null) {
            return ArithmeticOperator.LEFT_SHIFT;
        } else if (ctx.RIGHT_SHIFT() != null) {
            return ArithmeticOperator.RIGHT_SHIFT;
        } else if (ctx.BITWISE_AND() != null) {
            return ArithmeticOperator.BITWISE_AND;
        } else if (ctx.BITWISE_OR() != null) {
            return ArithmeticOperator.BITWISE_OR;
        } else {
            throw new IllegalArgumentException("Unsupported operate.");
        }
    }

    @Override
    public PredicateOperator visitPredicateOperator(
            EvalExpressionParser.PredicateOperatorContext ctx) {
        if (ctx.AND() != null) {
            return PredicateOperator.AND;
        }
        return PredicateOperator.OR;
    }

    // ======================================================================================
    // ----------------------------------- private helper -----------------------------------

    private BooleanExpression visitBooleanExpression(
            EvalExpressionParser.BooleanExpressionContext ctx) {
        if (ctx instanceof EvalExpressionParser.LogicalComparisonContext) {
            EvalExpressionParser.LogicalComparisonContext context =
                    (EvalExpressionParser.LogicalComparisonContext) ctx;
            BooleanExpression left = visitBooleanExpression(context.left);
            BooleanExpression right = visitBooleanExpression(context.right);
            PredicateOperator operator = visitPredicateOperator(context.operator);
            return new BooleanExpression.PredicateExpression(operator, left, right);
        } else if (ctx instanceof EvalExpressionParser.ValueComparisonContext) {
            return visitValueComparison((EvalExpressionParser.ValueComparisonContext) ctx);
        } else {
            return visitNotComparison((EvalExpressionParser.NotComparisonContext) ctx);
        }
    }

    private ValueExpression visitValueExpression(EvalExpressionParser.ValueExpressionContext ctx) {
        if (ctx instanceof EvalExpressionParser.ConstantValueContext) {
            return visitConstantValue((EvalExpressionParser.ConstantValueContext) ctx);
        } else if (ctx instanceof EvalExpressionParser.VariableContext) {
            return visitVariable((EvalExpressionParser.VariableContext) ctx);
        } else {
            return visitArithmeticValue((EvalExpressionParser.ArithmeticValueContext) ctx);
        }
    }
}
