package com.geedgenetworks.core.processor.split;

import com.geedgenetworks.api.expressions.AviatorExecutor;
import com.geedgenetworks.api.metrics.InternalMetrics;
import com.geedgenetworks.api.event.Event;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.AviatorEvaluatorInstance;
import com.googlecode.aviator.Expression;
import com.googlecode.aviator.Options;
import com.googlecode.aviator.exception.ExpressionRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;


@Slf4j
public class SplitFunction extends ProcessFunction<Event, Event> {
    private final SplitConfig splitConfig;
    private List<RuleContext> rules;
    private transient InternalMetrics internalMetrics;

    public SplitFunction(SplitConfig splitConfig) {
        this.splitConfig = splitConfig;

    }

    @Override
    public void open(Configuration parameters) throws Exception {

        this.internalMetrics = new InternalMetrics(getRuntimeContext());
        this.rules = splitConfig.getRules();
        for(RuleContext rule : rules){
            rule.setAviatorExecutor(new AviatorExecutor(rule.getExpression()));
            OutputTag<Event> outputTag = new OutputTag<>(rule.getTag()){};
            rule.setOutputTag(outputTag);
        }
    }


    @Override
    public void processElement(Event event, ProcessFunction<Event, Event>.Context ctx, Collector<Event> out) throws Exception {
        try {
            internalMetrics.incrementInEvents();
            for (RuleContext route : rules){
                boolean result = route.getAviatorExecutor() != null ? route.getAviatorExecutor().filter(event.getExtractedFields()): true;
                if (result) {
                    ctx.output(route.getOutputTag(), event);
                }
            }
        } catch (Exception e) {
            internalMetrics.incrementErrorEvents();
            log.error("error in split function", e);
        }
    }

}
