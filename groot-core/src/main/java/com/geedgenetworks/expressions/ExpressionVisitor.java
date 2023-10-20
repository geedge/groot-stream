package com.geedgenetworks.expressions;

public interface ExpressionVisitor<R> {

    R visit(Expression expression);

    R visit(ValueExpression expression);

    R visit(ValueExpression.FieldRefExpression expression);

    R visit(ValueExpression.StringConstantExpression expression);

    R visit(ValueExpression.NumberConstantExpression expression);

    R visit(ValueExpression.BooleanConstantExpression expression);

    R visit(ValueExpression.ArithmeticExpression expression);

    R visit(ValueExpression.TernaryExpression expression);

    R visit(BooleanExpression expression);

    R visit(BooleanExpression.ComparisonExpression expression);

    R visit(BooleanExpression.PredicateExpression expression);

    R visit(BooleanExpression.NotExpression expression);
}
