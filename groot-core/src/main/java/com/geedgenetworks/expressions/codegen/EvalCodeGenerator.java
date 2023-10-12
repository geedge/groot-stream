package com.geedgenetworks.expressions.codegen;

import com.geedgenetworks.expressions.*;

import static com.geedgenetworks.expressions.codegen.CodeGeneratorContext.PARAM_NAME;

/**
 * Generating evaluation code for expressions.
 *
 * @author chaoc
 * @since 1.0
 */
public class EvalCodeGenerator implements ExpressionVisitor<String> {

    private final CodeGeneratorContext ctx;

    /**
     * Creates a new EvalCodeGenerator instance with the given code generation context.
     *
     * @param ctx The CodeGeneratorContext to use for code generation.
     */
    public EvalCodeGenerator(final CodeGeneratorContext ctx) {
        this.ctx = ctx;
    }

    @Override
    public String visit(Expression expression) {
        if (expression instanceof BooleanExpression) {
            return visit((BooleanExpression) expression);
        } else if (expression instanceof ValueExpression) {
            return visit((ValueExpression) expression);
        }
        throw new IllegalArgumentException("Unsupported expression type, " + expression.getClass());
    }

    @Override
    public String visit(ValueExpression expression) {
        if (expression instanceof ValueExpression.FieldRefExpression) {
            return visit((ValueExpression.FieldRefExpression) expression);
        } else if (expression instanceof ValueExpression.StringConstantExpression) {
            return visit((ValueExpression.StringConstantExpression) expression);
        } else if (expression instanceof ValueExpression.NumberConstantExpression) {
            return visit((ValueExpression.NumberConstantExpression) expression);
        } else if (expression instanceof ValueExpression.BooleanConstantExpression) {
            return visit((ValueExpression.BooleanConstantExpression) expression);
        } else if (expression instanceof ValueExpression.ArithmeticExpression) {
            return visit((ValueExpression.ArithmeticExpression) expression);
        } else if (expression instanceof ValueExpression.TernaryExpression) {
            return visit((ValueExpression.TernaryExpression) expression);
        }
        throw new IllegalArgumentException("Unsupported expression type, " + expression.getClass());
    }

    @Override
    public String visit(ValueExpression.FieldRefExpression expression) {
        String term = ctx.addReusableLocalVariable(expression.getFieldName());
        ctx.addEvalCodeStatements(
                String.format("%s = %s.get(\"%s\");", term, PARAM_NAME, expression.getFieldName()));
        return term;
    }

    @Override
    public String visit(ValueExpression.StringConstantExpression expression) {
        return ctx.addReusableConstant(expression.getValue());
    }

    @Override
    public String visit(ValueExpression.NumberConstantExpression expression) {
        return ctx.addReusableConstant(expression.getValue());
    }

    @Override
    public String visit(ValueExpression.BooleanConstantExpression expression) {
        return ctx.addReusableConstant(expression.getValue());
    }

    @Override
    public String visit(ValueExpression.ArithmeticExpression expression) {
        ArithmeticOperator operator = expression.getOperator();
        String leftTerm = visit(expression.getLeft());
        String rightTerm = visit(expression.getRight());
        String name = ctx.addLocalObjectVariable(true);
        String code = name + " = ";
        switch (operator) {
            case PLUS:
                code += String.format("%s + %s", leftTerm, rightTerm);
                break;
            case ASTERISK:
                code += String.format("%s * %s", leftTerm, rightTerm);
                break;
            case MINUS:
                code += String.format("%s - %s", leftTerm, rightTerm);
                break;
            case PERCENT:
                code += leftTerm + " % " + rightTerm;
                break;
            case SLASH:
                code += String.format("%s / %s", leftTerm, rightTerm);
                break;
        }
        ctx.addEvalCodeStatements(code + ";");
        return name;
    }

    @Override
    public String visit(ValueExpression.TernaryExpression expression) {
        String boolTerm = visit(expression.getExpression());
        String trueValueTerm = visit(expression.getTrueValue());
        String falseValueTerm = visit(expression.getFalseValue());
        String name = ctx.addLocalObjectVariable();
        ctx.addEvalCodeStatements(String.format("%s = (%s) ? %s : %s;",
                name, boolTerm, trueValueTerm, falseValueTerm));
        return name;
    }

    @Override
    public String visit(BooleanExpression expression) {
        if (expression instanceof BooleanExpression.ComparisonExpression) {
            return visit((BooleanExpression.ComparisonExpression) expression);
        } else if (expression instanceof BooleanExpression.PredicateExpression) {
            return visit((BooleanExpression.PredicateExpression) expression);
        } else if (expression instanceof BooleanExpression.NotExpression) {
            return visit((BooleanExpression.NotExpression) expression);
        }
        throw new IllegalArgumentException("Unsupported expression type, " + expression.getClass());
    }

    @Override
    public String visit(BooleanExpression.ComparisonExpression expression) {
        String leftTerm = visit(expression.getLeft());
        String rightTerm = visit(expression.getRight());
        String term = ctx.addLocalBoolVariable();
        String code = String.format("%s = %s;", term, false);
        String temp =
                "if ( " + leftTerm + " instanceof java.lang.Number && " + rightTerm + " instanceof java.lang.Number ) {\n" +
                        "   " + term + " = ( (java.lang.Number) " + leftTerm + " ).doubleValue()" +
                        " %s ( (java.lang.Number) " + rightTerm + " ).doubleValue();\n" +
                        "}";
        switch (expression.getOperator()) {
            case EQ:
                code = String.format("%s = java.util.Objects.equals(%s, %s);", term, leftTerm, rightTerm);
                break;
            case NEQ:
                code = String.format("%s = !java.util.Objects.equals(%s, %s);", term, leftTerm, rightTerm);
                break;
            case LTE:
                code = String.format(temp, "<=");
                break;
            case LT:
                code = String.format(temp, "<");
                break;
            case GTE:
                code = String.format(temp, ">=");
                break;
            case GT:
                code = String.format(temp, ">");
                break;
        }
        ctx.addEvalCodeStatements(code);
        return term;
    }

    @Override
    public String visit(BooleanExpression.PredicateExpression expression) {
        String leftTerm = visit(expression.getLeft());
        String rightTerm = visit(expression.getRight());
        String term = ctx.addLocalBoolVariable();
        String code = "";
        switch (expression.getOperator()) {
            case AND:
                code = String.format("%s = %s && %s;", term, leftTerm, rightTerm);
                break;
            case OR:
                code = String.format("%s = %s || %s;", term, leftTerm, rightTerm);
                break;
        }
        ctx.addEvalCodeStatements(code);
        return term;
    }

    @Override
    public String visit(BooleanExpression.NotExpression expression) {
        String term = visit(expression.getExpression());
        String retTerm = ctx.addLocalBoolVariable();
        ctx.addEvalCodeStatements(String.format("%s = ! %s;", retTerm, term));
        return retTerm;
    }
}