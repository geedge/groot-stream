package com.geedgenetworks.core.processor.split;

import com.geedgenetworks.api.connector.event.Event;
import com.googlecode.aviator.Expression;
import lombok.Data;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;

@Data
public class RuleContext implements Serializable {

    private String tag;
    private String expression;
    private Expression compiledExpression;
    private OutputTag<Event> outputTag ;


}
