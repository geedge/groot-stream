package com.geedgenetworks.core.processor.split;

import com.geedgenetworks.api.event.Event;
import com.geedgenetworks.api.expressions.AviatorExecutor;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import lombok.Data;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;

@Data
public class RuleContext implements Serializable {

    private String tag;
    private String expression;
    private AviatorExecutor aviatorExecutor;
    private OutputTag<Event> outputTag ;


}
