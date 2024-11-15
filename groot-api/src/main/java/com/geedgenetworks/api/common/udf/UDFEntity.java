package com.geedgenetworks.api.common.udf;
import com.googlecode.aviator.Expression;
import lombok.Data;

import java.io.Serializable;

@Data
public class UDFEntity implements Serializable {
    private ScalarFunction scalarFunction;
    private AggregateFunction aggregateFunction;
    private TableFunction tableFunction;
    private Expression filterExpression;
    private String name;
    private String className;
    private UDFContext udfContext;
}
