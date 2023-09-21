package com.geedgenetworks.expressions;


import java.util.List;

public interface Expression {

    List<Expression> getChildren();

    <R> R accept(ExpressionVisitor<R> visitor);
}
