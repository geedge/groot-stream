package com.geedgenetworks.expressions;

import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Value expression.
 *
 * @author chaoc
 * @since 1.0
 */
public abstract class ValueExpression implements Expression {

    public static class FieldRefExpression extends ValueExpression {

        private final String fieldName;

        public FieldRefExpression(String fieldName) {
            this.fieldName = fieldName;
        }

        public String getFieldName() {
            return fieldName;
        }

        @Override
        public List<Expression> getChildren() {
            return Collections.emptyList();
        }

        @Override
        public <R> R accept(ExpressionVisitor<R> visitor) {
            return visitor.visit(this);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FieldRefExpression that = (FieldRefExpression) o;
            return Objects.equals(fieldName, that.fieldName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldName);
        }

        @Override
        public String toString() {
            return "FieldRef[" + fieldName + ']';
        }
    }

    public static class ArithmeticExpression extends ValueExpression {

        private final ArithmeticOperator operator;
        private final ValueExpression left;
        private final ValueExpression right;

        public ArithmeticExpression(ArithmeticOperator operator, ValueExpression left, ValueExpression right) {
            this.operator = operator;
            this.left = left;
            this.right = right;
        }

        public ArithmeticOperator getOperator() {
            return operator;
        }

        public ValueExpression getLeft() {
            return left;
        }

        public ValueExpression getRight() {
            return right;
        }

        @Override
        public List<Expression> getChildren() {
            return Lists.newArrayList(left, right);
        }

        @Override
        public <R> R accept(ExpressionVisitor<R> visitor) {
            return visitor.visit(this);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ArithmeticExpression that = (ArithmeticExpression) o;
            return operator == that.operator && Objects.equals(left, that.left) && Objects.equals(right, that.right);
        }

        @Override
        public int hashCode() {
            return Objects.hash(operator, left, right);
        }

        @Override
        public String toString() {
            return "[" + left + " " + operator + " " + right + "]";

        }
    }

    public static class TernaryExpression extends ValueExpression {

        private final BooleanExpression expression;
        private final ValueExpression trueValue;
        private final ValueExpression falseValue;

        public TernaryExpression(BooleanExpression expression, ValueExpression trueValue, ValueExpression falseValue) {
            this.expression = expression;
            this.trueValue = trueValue;
            this.falseValue = falseValue;
        }

        public BooleanExpression getExpression() {
            return expression;
        }

        public ValueExpression getTrueValue() {
            return trueValue;
        }

        public ValueExpression getFalseValue() {
            return falseValue;
        }

        @Override
        public List<Expression> getChildren() {
            return Lists.newArrayList(expression, trueValue, falseValue);
        }

        @Override
        public <R> R accept(ExpressionVisitor<R> visitor) {
            return visitor.visit(this);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TernaryExpression that = (TernaryExpression) o;
            return Objects.equals(expression, that.expression)
                    && Objects.equals(trueValue, that.trueValue) && Objects.equals(falseValue, that.falseValue);
        }

        @Override
        public int hashCode() {
            return Objects.hash(expression, trueValue, falseValue);
        }

        @Override
        public String toString() {
            return "[" + expression + " ? " + trueValue + " : " + falseValue + ']';
        }
    }

    public static abstract class ConstantValueExpression<T> extends ValueExpression {

        private final T value;

        public ConstantValueExpression(final T value) {
            this.value = value;
        }

        public T getValue() {
            return value;
        }

        @Override
        public List<Expression> getChildren() {
            return Collections.emptyList();
        }

        @Override
        public <R> R accept(ExpressionVisitor<R> visitor) {
            return visitor.visit(this);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ConstantValueExpression<?> that = (ConstantValueExpression<?>) o;
            return Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(value);
        }

        @Override
        public String toString() {
            return "ConstantValue[" + value + "]";
        }
    }

    public static class BooleanConstantExpression extends ConstantValueExpression<Boolean> {

        public BooleanConstantExpression(final Boolean value) {
            super(value);
        }
    }

    public static class StringConstantExpression extends ConstantValueExpression<String> {

        public StringConstantExpression(final String value) {
            super(value);
        }
    }

    public static class NumberConstantExpression extends ConstantValueExpression<Number> {

        public NumberConstantExpression(final Number value) {
            super(value);
        }
    }
}
