package com.geedgenetworks.expressions;

import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Boolean expression.
 *
 * @author chaoc
 * @since 1.0
 */
public abstract class BooleanExpression implements Expression {

    public static class ComparisonExpression extends BooleanExpression {

        private final ComparisonOperator operator;

        private final ValueExpression left;
        private final ValueExpression right;

        public ComparisonExpression(
                ComparisonOperator operator, ValueExpression left, ValueExpression right) {
            this.operator = operator;
            this.left = left;
            this.right = right;
        }

        public ComparisonOperator getOperator() {
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
            ComparisonExpression that = (ComparisonExpression) o;
            return operator == that.operator
                    && Objects.equals(left, that.left)
                    && Objects.equals(right, that.right);
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

    public static class PredicateExpression extends BooleanExpression {

        private final PredicateOperator operator;

        private final BooleanExpression left;
        private final BooleanExpression right;

        public PredicateExpression(
                PredicateOperator operator, BooleanExpression left, BooleanExpression right) {
            this.operator = operator;
            this.left = left;
            this.right = right;
        }

        public PredicateOperator getOperator() {
            return operator;
        }

        public BooleanExpression getLeft() {
            return left;
        }

        public BooleanExpression getRight() {
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
            PredicateExpression that = (PredicateExpression) o;
            return operator == that.operator
                    && Objects.equals(left, that.left)
                    && Objects.equals(right, that.right);
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

    public static class NotExpression extends BooleanExpression {

        private final BooleanExpression expression;

        public NotExpression(BooleanExpression expression) {
            this.expression = expression;
        }

        public BooleanExpression getExpression() {
            return expression;
        }

        @Override
        public List<Expression> getChildren() {
            return Collections.singletonList(expression);
        }

        @Override
        public <R> R accept(ExpressionVisitor<R> visitor) {
            return visitor.visit(expression);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NotExpression that = (NotExpression) o;
            return Objects.equals(expression, that.expression);
        }

        @Override
        public int hashCode() {
            return Objects.hash(expression);
        }

        @Override
        public String toString() {
            return "! " + expression;
        }
    }
}
