package com.geedgenetworks.core.processor.filter;

import com.geedgenetworks.common.utils.ColumnUtil;
import com.geedgenetworks.api.metrics.InternalMetrics;
import com.geedgenetworks.api.connector.event.Event;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.AviatorEvaluatorInstance;
import com.googlecode.aviator.Expression;
import com.googlecode.aviator.Options;
import com.googlecode.aviator.exception.ExpressionRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;

import java.util.Map;
@Slf4j
public class FilterFunction extends RichFilterFunction<Event> {
    private final FilterConfig filterConfig;
    private static Expression compiledExp;
    private static String expression;
    private transient InternalMetrics internalMetrics;

    public FilterFunction(FilterConfig filterConfig) {
        this.filterConfig = filterConfig;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        this.internalMetrics = new InternalMetrics(getRuntimeContext());
        expression = filterConfig.getProperties().getOrDefault("expression", "").toString();
        AviatorEvaluatorInstance instance = AviatorEvaluator.getInstance();
        instance.setCachedExpressionByDefault(true);
        instance.setOption(Options.OPTIMIZE_LEVEL, AviatorEvaluator.EVAL);
        instance.setFunctionMissing(null);
        compiledExp = instance.compile(expression, true);
    }

    @Override
    public boolean filter(Event value)  {
        internalMetrics.incrementInEvents();
        boolean isFilter ;
        try {

            if (filterConfig.getOutput_fields() != null
                    && !filterConfig.getOutput_fields().isEmpty()) {
                value.setExtractedFields(
                        ColumnUtil.columnSelector(
                                value.getExtractedFields(), filterConfig.getOutput_fields()));
            }
            isFilter = aviatorExcute(value.getExtractedFields());

        } catch (ExpressionRuntimeException e){
            isFilter = false;
            log.error("Invalid filter ! expression=" +expression);
            internalMetrics.incrementErrorEvents();
        }
        catch (RuntimeException ignored) {
            isFilter = false;
            internalMetrics.incrementErrorEvents();
        }
        if(isFilter){
            internalMetrics.incrementOutEvents();
        }
        else {
            internalMetrics.incrementDroppedEvents();
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
