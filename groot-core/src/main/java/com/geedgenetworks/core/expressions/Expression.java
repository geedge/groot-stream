package com.geedgenetworks.core.expressions;

import java.util.List;

public interface Expression {

    List<Expression> getChildren();

    <R> R accept(ExpressionVisitor<R> visitor);
}
