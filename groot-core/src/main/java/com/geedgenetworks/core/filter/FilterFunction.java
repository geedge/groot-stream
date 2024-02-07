package com.geedgenetworks.core.filter;

import com.geedgenetworks.common.utils.ColumnUtil;
import com.geedgenetworks.common.Event;
import com.geedgenetworks.core.pojo.FilterConfig;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.AviatorEvaluatorInstance;
import com.googlecode.aviator.Expression;
import com.googlecode.aviator.Options;
import com.googlecode.aviator.exception.ExpressionRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;

import java.util.Map;
@Slf4j
public class FilterFunction extends RichFilterFunction<Event> {
    private final com.geedgenetworks.core.pojo.FilterConfig FilterConfig;
    private static Expression compiledExp;
    private static String expression;
    private transient Counter droppedEvents;
    private transient Counter errorEvents;
    private transient Counter outEvents;
    private transient Counter inEvents;
    public FilterFunction(FilterConfig FilterConfig) {
        this.FilterConfig = FilterConfig;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        expression = FilterConfig.getProperties().getOrDefault("expression", "").toString();
        AviatorEvaluatorInstance instance = AviatorEvaluator.getInstance();
        instance.setCachedExpressionByDefault(true);
        instance.setOption(Options.OPTIMIZE_LEVEL, AviatorEvaluator.EVAL);
        instance.setFunctionMissing(null);
        compiledExp = instance.compile(expression, true);
        this.errorEvents = getRuntimeContext().getMetricGroup().counter("error_events");
        this.droppedEvents = getRuntimeContext().getMetricGroup().counter("dropped_events");
        this.inEvents = getRuntimeContext().getMetricGroup().counter("in_events");
        this.outEvents = getRuntimeContext().getMetricGroup().counter("out_events");
    }

    @Override
    public boolean filter(Event value)  {
        inEvents.inc();
        boolean isFilter ;
        try {

            if (FilterConfig.getOutput_fields() != null
                    && !FilterConfig.getOutput_fields().isEmpty()) {
                value.setExtractedFields(
                        ColumnUtil.columnSelector(
                                value.getExtractedFields(), FilterConfig.getOutput_fields()));
            }
            isFilter = aviatorExcute(value.getExtractedFields());

        } catch (ExpressionRuntimeException e){
            isFilter = false;
            log.error("Invalid filter ! expression=" +expression);
            errorEvents.inc();
        }
        catch (RuntimeException ignored) {
            isFilter = false;
            errorEvents.inc();
        }
        if(isFilter){
            outEvents.inc();
        }
        else {
            droppedEvents.inc();
        }
        return isFilter;
    }
    public static boolean aviatorExcute(Map<String, Object> map) {

        boolean bool = true;
        if (!"".equals(expression)) {
            Object object = compiledExp.execute(compiledExp.newEnv("event", map));
            if(object != null){
                bool = (boolean) object;
            }
            else {
                throw new ExpressionRuntimeException();
            }
        }
        return bool;

    }
}
