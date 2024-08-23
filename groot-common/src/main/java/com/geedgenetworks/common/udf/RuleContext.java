package com.geedgenetworks.common.udf;

import com.geedgenetworks.common.Event;
import com.googlecode.aviator.Expression;
import lombok.Data;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;

@Data
public class RuleContext implements Serializable {

    private String name;
    private String expression;
    private Expression compiledExpression;
    private OutputTag<Event> outputTag ;


}
