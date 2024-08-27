package com.geedgenetworks.core.processor.projection;

import com.geedgenetworks.common.udf.AggregateFunction;
import com.geedgenetworks.common.udf.ScalarFunction;

import com.geedgenetworks.common.udf.TableFunction;
import com.geedgenetworks.common.udf.UDFContext;
import com.googlecode.aviator.Expression;
import lombok.Data;

import java.io.Serializable;

@Data
public class UdfEntity implements Serializable {
    private ScalarFunction scalarFunction;
    private AggregateFunction aggregateFunction;
    private TableFunction tableFunction;
    private Expression filterExpression;
    private String name;
    private String className;
    private UDFContext udfContext;
}
